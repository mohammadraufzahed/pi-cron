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
 *   daily:HH:MM        — once a day at host-local HH:MM
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
const DAEMON = join(dirname(fileURLToPath(import.meta.url)), "daemon.mjs");

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
	const child = spawn(process.execPath, [DAEMON], {
		detached: true,
		stdio: "ignore",
		env: { ...process.env, PI_CRON_DIR: DIR },
	});
	child.unref();
}

const SPEC_RE =
	/^(every:\d+|in:\d+|daily:\d{1,2}:\d{2}|once:\d+|cron:.+)$/;

interface Job {
	id: string;
	soul: string;
	spec: string;
	prompt: string;
	chat?: string;
	thread?: number;
	silent?: boolean;
	next_run: number;
	last_run?: number;
	runs?: number;
	fails?: number;
	paused?: boolean;
	created: number;
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
			(job.last_run ? ` last=${fmtTs(job.last_run)}` : ""),
		`  prompt: ${job.prompt.slice(0, 100)}`,
	].join("\n");
}

export default function piCron(pi: ExtensionAPI) {
	pi.registerTool({
		name: "cron_add",
		label: "Cron Add",
		description:
			"Schedule work: 'every:90' (repeat), 'in:30' (one-shot in 30m), " +
			"'daily:09:30' (host-local), 'once:UNIX_TS', or 'cron:0 9 * * 1-5'. " +
			"Set soul to schedule for a teammate, silent=true to run without posting.",
		promptSnippet: "Schedule a recurring or one-shot task",
		parameters: Type.Object({
			spec: Type.String({ description: "every:N | in:N | daily:HH:MM | once:TS | cron:EXPR" }),
			prompt: Type.String({ description: "What to do when it fires" }),
			soul: Type.Optional(
				Type.String({ description: "Which soul runs it (default: you)" }),
			),
			silent: Type.Optional(
				Type.Boolean({ description: "Run without posting the result" }),
			),
		}),
		async execute(_id, params) {
			const spec = params.spec.trim();
			if (!SPEC_RE.test(spec)) {
				return {
					content: [
						{
							type: "text" as const,
							text: `bad spec '${spec}' — use every:N | in:N | daily:HH:MM | once:TS | cron:EXPR`,
						},
					],
				};
			}
			ensureDaemon();
			const job: Job = {
				id: randomUUID(),
				soul: params.soul ?? process.env.PI_TEAM_FROM ?? "unknown",
				env: envSnapshot(),
				cwd: process.cwd(),
				spec: spec.startsWith("in:")
					? `once:${Math.floor(Date.now() / 1000) + parseInt(spec.slice(3)) * 60}`
					: spec,
				prompt: params.prompt,
				chat: process.env.PI_TEAM_CHAT,
				thread: process.env.PI_TEAM_THREAD
					? parseInt(process.env.PI_TEAM_THREAD)
					: undefined,
				silent: params.silent,
				next_run: spec.startsWith("in:")
					? Math.floor(Date.now() / 1000) + parseInt(spec.slice(3)) * 60
					: 0, // host computes on first scan
				created: Math.floor(Date.now() / 1000),
			};
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
}
