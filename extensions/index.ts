/**
 * pi-cron — self-scheduling for the pi coding agent.
 *
 * Souls create their own recurring/one-shot jobs — the host process
 * runs them later as that soul:
 *
 *   cron_add     — schedule a job: every N minutes, daily HH:MM, or once
 *   cron_list    — your scheduled jobs
 *   cron_remove  — delete a job by id
 *
 * Store: $PI_CRON_DIR (default ~/.local/state/telegram-agent/cron)
 *   jobs/<id>.json — {id, soul, kind, spec, prompt, chat, thread,
 *                     next_run, created}
 * The host watcher fires due jobs and updates next_run.
 *
 * Schedule formats (kind=spec):
 *   "every:90"      — every 90 minutes
 *   "daily:09:30"   — once a day at 09:30 (host-local time)
 *   "once:1696089600" — unix timestamp, fires once then deleted
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { existsSync, mkdirSync, readdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { randomUUID } from "node:crypto";
import { Type } from "typebox";

const CRON_DIR =
	process.env.PI_CRON_DIR ??
	join(homedir(), ".local/state/telegram-agent/cron");
const JOBS = join(CRON_DIR, "jobs");

interface Job {
	id: string;
	soul: string;
	spec: string;
	prompt: string;
	chat?: string;
	thread?: string;
	next_run: number; // unix seconds
	created: number;
}

function nextRun(spec: string): number | null {
	const now = Math.floor(Date.now() / 1000);
	const [kind, arg] = spec.split(":", 2);
	if (kind === "every") {
		const mins = Number(arg);
		return mins > 0 ? now + mins * 60 : null;
	}
	if (kind === "daily") {
		const [h, m] = arg.split(":").map(Number);
		const d = new Date();
		d.setHours(h, m, 0, 0);
		if (d.getTime() / 1000 <= now) d.setDate(d.getDate() + 1);
		return Math.floor(d.getTime() / 1000);
	}
	if (kind === "once") return Number(arg) > now ? Number(arg) : null;
	return null;
}

function list(): Job[] {
	mkdirSync(JOBS, { recursive: true });
	return readdirSync(JOBS)
		.filter((f) => f.endsWith(".json"))
		.map((f) => {
			try {
				return JSON.parse(readFileSync(join(JOBS, f), "utf-8")) as Job;
			} catch {
				return null;
			}
		})
		.filter(Boolean) as Job[];
}

export default function piCron(pi: ExtensionAPI) {
	const me = () => process.env.PI_TEAM_FROM ?? "agent";

	pi.registerTool({
		name: "cron_add",
		label: "Cron Add",
		description:
			"Schedule a job that runs later AS YOU (same persona/tools). Specs: 'every:90' (minutes), 'daily:09:30', 'once:<unix-ts>'.",
		promptSnippet: "Schedule a recurring or one-shot job",
		promptGuidelines: [
			"Use cron_add for 'check X every morning' / 'remind me' — the host runs it as you later.",
		],
		parameters: Type.Object({
			spec: Type.String({ description: "every:MIN | daily:HH:MM | once:TS" }),
			prompt: Type.String({ description: "What to do when it fires" }),
		}),
		async execute(_id, params) {
			const nr = nextRun(params.spec);
			if (nr === null)
				return {
					content: [
						{
							type: "text" as const,
							text: `bad spec '${params.spec}' — use every:MIN | daily:HH:MM | once:UNIX_TS`,
						},
					],
				};
			mkdirSync(JOBS, { recursive: true });
			const job: Job = {
				id: randomUUID().slice(0, 8),
				soul: me(),
				spec: params.spec,
				prompt: params.prompt,
				chat: process.env.PI_TEAM_CHAT,
				thread: process.env.PI_TEAM_THREAD,
				next_run: nr,
				created: Math.floor(Date.now() / 1000),
			};
			writeFileSync(join(JOBS, `${job.id}.json`), JSON.stringify(job));
			return {
				content: [
					{
						type: "text" as const,
						text: `scheduled ${job.id} (${params.spec}) — next run ${new Date(nr * 1000).toISOString()}`,
					},
				],
			};
		},
	});

	pi.registerTool({
		name: "cron_list",
		label: "Cron List",
		description: "List YOUR scheduled jobs (and the whole team's).",
		parameters: Type.Object({}),
		async execute() {
			const jobs = list();
			if (!jobs.length)
				return {
					content: [{ type: "text" as const, text: "(no jobs)" }],
				};
			return {
				content: [
					{
						type: "text" as const,
						text: jobs
							.map(
								(j) =>
									`${j.id} [${j.soul}] ${j.spec} next=${new Date(j.next_run * 1000).toISOString()} — ${j.prompt.slice(0, 80)}`,
							)
							.join("\n"),
					},
				],
			};
		},
	});

	pi.registerTool({
		name: "cron_remove",
		label: "Cron Remove",
		description: "Delete a scheduled job by id.",
		parameters: Type.Object({ id: Type.String() }),
		async execute(_id, params) {
			const f = join(JOBS, `${params.id}.json`);
			if (!existsSync(f))
				return {
					content: [{ type: "text" as const, text: "no such job" }],
				};
			unlinkSync(f);
			return {
				content: [{ type: "text" as const, text: `removed ${params.id}` }],
			};
		},
	});
}
