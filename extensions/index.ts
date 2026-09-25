/**
 * pi-cron — self-scheduling for pi agents (v2).
 *
 * Souls schedule recurring/one-shot work for themselves or teammates.
 * Jobs are JSON files in $PI_CRON_DIR/jobs/ (default
 * ~/.local/state/telegram-agent/cron/jobs); the host's cron watcher
 * fires due jobs AS their soul and posts the result via its bot.
 *
 * Specs:
 *   every:MINUTES      — repeat every N minutes
 *   in:MINUTES         — one-shot, N minutes from now
 *   daily:HH:MM[@TZ]   — once a day at HH:MM in TZ (default Asia/Tehran)
 *   once:UNIX_TS       — one-shot at a timestamp
 *   cron:M H DOM MON DOW — classic 5-field cron (host-local)
 *
 * Job fields: id, soul, spec, prompt, chat, thread, silent,
 *             next_run, last_run, runs, fails, paused, created
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import {
	existsSync,
	mkdirSync,
	readFileSync,
	readdirSync,
	writeFileSync,
	unlinkSync,
} from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { Type } from "typebox";

const DIR =
	process.env.PI_CRON_DIR ??
	join(homedir(), ".local/state/pi-cron");
const JOBS = join(DIR, "jobs");
const DAEMON = join(dirname(fileURLToPath(import.meta.url)), "daemon.py");

/** Env worth snapshotting into the job — identity + delivery + cwd. */
const ENV_KEYS = [
	"PI_TEAM_DIR", "PI_TEAM_FROM", "PI_TEAM_CHAT", "PI_TEAM_THREAD",
	"PI_TEAM_MSG", "TG_BOT_TOKEN", "TG_CHAT", "TG_THREAD",
	"GH_TOKEN", "GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL",
	"GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL", "SOULS_DIR",
	"PI_CRON_DIR", "PI_MODEL", "PI_MODEL_CHAT", "OPENROUTER_API_KEY",
];

function envSnapshot(): Record<string, string> {
	const env: Record<string, string> = {};
	for (const k of ENV_KEYS) {
		const v = process.env[k];
		if (v) env[k] = v;
	}
	return env;
}

/** Ensure the scheduler is running: prefer the systemd user unit
 *  (`make install`), fall back to a detached spawn. */
function ensureDaemon() {
	const pidfile = join(DIR, "daemon.pid");
	try {
		const pid = parseInt(readFileSync(pidfile, "utf-8"));
		process.kill(pid, 0);
		return; // alive
	} catch {
		/* stale/missing */
	}
	try {
		const r = spawn(
			"systemctl",
			["--user", "start", "pi-cron.service"],
			{ stdio: "ignore" },
		);
		r.unref();
		return;
	} catch {
		/* no systemd — detached fallback */
	}
	mkdirSync(DIR, { recursive: true });
	const child = spawn("python3", [DAEMON], {
		detached: true,
		stdio: "ignore",
		env: { ...process.env, PI_CRON_DIR: DIR },
	});
	child.unref();
}

const SPEC_RE =
	/^(every:\d+s?|in:\d+s?|daily:\d{1,2}:\d{2}(@[\w/+\-]+)?|once:\d+|cron:.+)$/;

interface Job {
	id: string;
	soul: string;
	spec: string;
	prompt: string;
	chat?: string;
	thread?: number;
	silent?: boolean;
	tools?: string;
	report?: string;         // always|on-failure|every:N|never
	project?: string;
	next_run: number;
	last_run?: number;
	last_status?: string; // ok | failed | timeout
	last_answer?: string; // first 200 chars of the last run's answer
	runs?: number;
	fails?: number;
	paused?: boolean;
	created: number;
	times?: number;
	dedup_key?: string; // souls pass e.g. 'pr:owner/repo#N' — re-adding replaces the older job
}

function loadJobs(): Job[] {
	if (!existsSync(JOBS)) return [];
	const out: Job[] = [];
	for (const f of readdirSync(JOBS)) {
		if (!f.endsWith(".json")) continue;
		try {
			out.push(JSON.parse(readFileSync(join(JOBS, f), "utf-8")));
		} catch {
			/* corrupt — skip */
		}
	}
	return out;
}

function saveJob(job: Job) {
	mkdirSync(JOBS, { recursive: true });
	writeFileSync(join(JOBS, `${job.id}.json`), JSON.stringify(job, null, 2));
}

function fmtTs(ts: number): string {
	return new Date(ts * 1000).toLocaleString("en-GB", { hour12: false });
}

function describe(job: Job): string {
	const flags = [
		job.paused ? "paused" : null,
		job.silent ? "silent" : null,
	].filter(Boolean);
	return [
		`${job.id} — ${job.spec}${flags.length ? ` [${flags.join(",")}]` : ""}`,
		`  soul=${job.soul} next=${fmtTs(job.next_run)}`,
		`  runs=${job.runs ?? 0} fails=${job.fails ?? 0}` +
			(job.last_run ? ` last=${job.last_status || "ok"}@${fmtTs(job.last_run)}` : ""),
		`  prompt: ${job.prompt.slice(0, 100)}`,
	].join("\n");
}

const WRITE_TOOLS = new Set([
	"edit", "write", "bash", "gh_issue_create", "gh_issue_comment",
	"gh_issue_close", "gh_pr_create", "gh_pr_comment", "gh_pr_merge",
	"gh_pr_review", "gh_comment_reply", "deps_update", "docker_exec",
	"project_register", "project_forget", "project_mode",
]);

async function projectUse(
	name: string,
): Promise<{ dir?: string; repo?: string; mode?: string } | null> {
	const teamDir = process.env.PI_TEAM_DIR;
	if (!teamDir) return null;
	try {
		const reqDir = join(teamDir, "requests");
		const repDir = join(teamDir, "replies");
		mkdirSync(reqDir, { recursive: true });
		mkdirSync(repDir, { recursive: true });
		const id = randomUUID();
		writeFileSync(
			join(reqDir, `${id}.json`),
			JSON.stringify({
				id,
				from: process.env.PI_TEAM_FROM ?? "?",
				to: "host",
				kind: "project",
				text: `use|||${name}`,
				at: Date.now(),
			}),
		);
		const file = join(repDir, `${id}.json`);
		const deadline = Date.now() + 15_000;
		while (Date.now() < deadline) {
			if (existsSync(file)) {
				const r = JSON.parse(readFileSync(file, "utf-8"));
				const dir = String(r.text ?? "").match(/dir=(\S+)/)?.[1];
				const repo = String(r.text ?? "").match(/repo=(\S+)/)?.[1];
				const mode = String(r.text ?? "").match(/mode=(\S+)/)?.[1];
				return { dir, repo: repo === "—" ? undefined : repo, mode };
			}
			await new Promise((r) => setTimeout(r, 400));
		}
	} catch {
		/* no host — skip */
	}
	return null;
}

export default function piCron(pi: ExtensionAPI) {
	pi.registerTool({
		name: "cron_add",
		label: "Cron Add",
		description:
			"Schedule work: 'every:90' (repeat), 'in:30' (one-shot in 30m), " +
			"'daily:09:30' (host-local), 'once:UNIX_TS', or 'cron:0 9 * * 1-5'. " +
			"Set soul to schedule for a teammate, silent=true to skip posting, times=N to cap repeats.",
		promptSnippet: "Schedule a recurring or one-shot task",
		parameters: Type.Object({
			spec: Type.String({ description: "every:N | in:N | daily:HH:MM[@TZ] | once:TS | cron:EXPR[@TZ] — e.g. daily:09:30@Asia/Tehran" }),
			prompt: Type.String({ description: "What to do when it fires" }),
			soul: Type.Optional(
				Type.String({ description: "Which soul runs it (default: you)" }),
			),
			project: Type.Optional(
				Type.String({
					description:
						"Bind the job to a project — its dir becomes cwd, its repo GH_REPO",
				}),
			),
			silent: Type.Optional(
				Type.Boolean({ description: "Run without posting the result" }),
			),
			times: Type.Optional(
				Type.Number({ description: "Run at most N times then auto-remove" }),
			),
			dedup_key: Type.Optional(
				Type.String({
					description:
						"Dedup key — a new job with the same key replaces the older one " +
						"(e.g. 'pr:owner/repo#N' for PR follow-ups). Use it.",
				}),
			),
			thread: Type.Optional(
				Type.Number({
					description:
						"Forum topic id to post results into (default: current topic). " +
						"Use tg_topics to find one — a project's topic keeps its jobs' output together.",
				}),
			),
		}),
		async execute(_id, params) {
			const spec = params.spec.trim();
			if (!SPEC_RE.test(spec)) {
				return {
					content: [
						{
							type: "text" as const,
							text: `bad spec '${spec}' — use every:N | in:N | daily:HH:MM[@TZ] | once:TS | cron:EXPR[@TZ]`,  
						},
					],
				};
			}
			ensureDaemon();
			// project binding — resolve its dir/repo now via the mailbox
			let jobEnv = envSnapshot();
			let jobCwd = process.cwd();
			let jobTools: string | undefined;
			if (params.project) {
				const proj = await projectUse(params.project);
				if (proj?.dir) {
					jobCwd = proj.dir;
					if (proj.repo) jobEnv.GH_REPO = proj.repo;
				}
				// lifecycle gate — a readonly/monitor/paused project strips
				// write tools from the job's allowlist
				if (proj?.mode && proj.mode !== "develop") {
					const strip = new Set(WRITE_TOOLS);
					if (proj.mode === "monitor")
						["deps_outdated", "git_diff"].forEach((t) => strip.add(t));
					if (proj.mode === "paused")
						["web_search", "web_fetch"].forEach((t) => strip.add(t));
					const allow = (process.env.PI_TOOLS ?? "")
						.split(",")
						.map((t) => t.trim())
						.filter(Boolean)
						.filter((t) => !strip.has(t));
					if (allow.length) jobTools = allow.join(",");
				}
			}
			const inArg = spec.slice(3);
			const inSecs = inArg.endsWith("s")
				? parseInt(inArg)
				: parseInt(inArg) * 60;
			let prompt = params.prompt;
			const policy = params.report ?? (params.silent ? "on-failure" : "always");
			if (params.silent || params.report) {
				prompt += `\n\n[Execution policy — ${policy}: this is a SILENT background job. Post nothing to chat unless the policy says to: 'always' post your result, 'on-failure' post ONLY if something is wrong/threshold crossed, 'every:N' post on the Nth consecutive run (use job_state for the counter), 'never' post nothing (internal work only). Working silently is the job.]`;
			}
			// dedup_key replaces an older pending job for the same thing —
			// 'check PR #N in 45m' should never stack into three copies.
			if (params.dedup_key) {
				for (const j of loadJobs()) {
					if (j.dedup_key === params.dedup_key) {
						try {
							unlinkSync(join(JOBS, `${j.id}.json`));
						} catch {}
					}
				}
			}
			const job: Job = {
				id: randomUUID(),
				soul: params.soul ?? process.env.PI_TEAM_FROM ?? "unknown",
				env: { ...jobEnv },
				cwd: jobCwd,
				spec: spec.startsWith("in:")
					? `once:${Math.floor(Date.now() / 1000) + inSecs}`
					: spec,
				prompt,
				chat: process.env.PI_TEAM_CHAT,
				thread: params.thread ?? (process.env.PI_TEAM_THREAD
					? parseInt(process.env.PI_TEAM_THREAD)
					: undefined),
				dedup_key: params.dedup_key,
				silent: params.silent,
				tools: jobTools,
				report: params.report,
				times: params.times,
				project: params.project,
				next_run: spec.startsWith("in:")
					? Math.floor(Date.now() / 1000) + inSecs
					: 0, // host computes on first scan
				created: Math.floor(Date.now() / 1000),
			};
			job.env.PI_JOB_ID = job.id;
			if (job.silent) job.env.PI_SILENT = "1";
			if (job.report) job.env.PI_REPORT = job.report;
			saveJob(job);
			return {
				content: [
					{
						type: "text" as const,
						text: `scheduled ${job.id.slice(0, 8)} — ${job.spec} (soul=${job.soul}${job.silent ? ", silent" : ""})`,
					},
				],
			};
		},
	});

	pi.registerTool({
		name: "cron_list",
		label: "Cron List",
		description: "List scheduled jobs — yours or everyone's.",
		promptSnippet: "List scheduled jobs",
		parameters: Type.Object({
			all: Type.Optional(Type.Boolean({ description: "All souls' jobs" })),
		}),
		async execute(_id, params) {
			const me = process.env.PI_TEAM_FROM ?? "";
			const jobs = loadJobs().filter(
				(j) => params.all || !me || j.soul === me,
			);
			const text = jobs.length
				? jobs.map(describe).join("\n\n")
				: "(no jobs)";
			return { content: [{ type: "text" as const, text }] };
		},
	});

	pi.registerTool({
		name: "cron_pause",
		label: "Cron Pause/Resume",
		description: "Pause or resume a scheduled job without deleting it.",
		parameters: Type.Object({
			id: Type.String({ description: "job id (or prefix)" }),
			paused: Type.Optional(Type.Boolean({ description: "default true" })),
		}),
		async execute(_id, params) {
			const job = loadJobs().find((j) => j.id.startsWith(params.id));
			if (!job)
				return {
					content: [{ type: "text" as const, text: `no job matching ${params.id}` }],
				};
			job.paused = params.paused ?? true;
			saveJob(job);
			return {
				content: [
					{
						type: "text" as const,
						text: `${job.id.slice(0, 8)} ${job.paused ? "paused" : "resumed"}`,
					},
				],
			};
		},
	});

	pi.registerTool({
		name: "cron_edit",
		label: "Cron Edit",
		description: "Change a job's spec or prompt.",
		parameters: Type.Object({
			id: Type.String({ description: "job id (or prefix)" }),
			spec: Type.Optional(Type.String()),
			prompt: Type.Optional(Type.String()),
		}),
		async execute(_id, params) {
			const job = loadJobs().find((j) => j.id.startsWith(params.id));
			if (!job)
				return {
					content: [{ type: "text" as const, text: `no job matching ${params.id}` }],
				};
			if (params.spec) {
				if (!SPEC_RE.test(params.spec))
					return {
						content: [
							{ type: "text" as const, text: `bad spec '${params.spec}'` },
						],
					};
				job.spec = params.spec;
				job.next_run = 0; // host recomputes
			}
			if (params.prompt) job.prompt = params.prompt;
			saveJob(job);
			return {
				content: [
					{ type: "text" as const, text: `${job.id.slice(0, 8)} updated` },
				],
			};
		},
	});

	pi.registerTool({
		name: "cron_remove",
		label: "Cron Remove",
		description: "Delete a scheduled job.",
		parameters: Type.Object({
			id: Type.String({ description: "job id (or prefix)" }),
		}),
		async execute(_id, params) {
			const job = loadJobs().find((j) => j.id.startsWith(params.id));
			if (!job)
				return {
					content: [{ type: "text" as const, text: `no job matching ${params.id}` }],
				};
			unlinkSync(join(JOBS, `${job.id}.json`));
			return {
				content: [
					{ type: "text" as const, text: `removed ${job.id.slice(0, 8)}` },
				],
			};
		},
	});

	pi.registerTool({
		name: "job_state",
		label: "Job State",
		description:
			"Persistent per-job memory — read/update a JSON blob that survives between this job's runs. Counters (every:N), last-seen, dedup hashes.",
		parameters: Type.Object({
			set: Type.Optional(
				Type.Object({}, { additionalProperties: true }),
			),
		}),
		async execute(_id, params) {
			const jid = process.env.PI_JOB_ID ?? "adhoc";
			const statesDir = join(DIR, "states");
			mkdirSync(statesDir, { recursive: true });
			const file = join(statesDir, `${jid}.json`);
			let state: Record<string, unknown> = {};
			if (existsSync(file)) {
				try {
					state = JSON.parse(readFileSync(file, "utf-8"));
				} catch {
					/* fresh */
				}
			}
			if (params.set && Object.keys(params.set).length) {
				state = { ...state, ...params.set };
				writeFileSync(file, JSON.stringify(state));
			}
			return {
				content: [
					{
						type: "text" as const,
						text: `job ${jid} state:\n${JSON.stringify(state, null, 2)}`,
					},
				],
			};
		},
	});
}
