#!/usr/bin/env node
/**
 * pi-cron daemon — self-contained scheduler for pi-cron jobs.
 *
 * Spawned detached by the extension on first cron_add (ensureDaemon).
 * Scans $PI_CRON_DIR/jobs every 30s; due jobs run `pi -p "<prompt>"`
 * as a subprocess with the env snapshot captured at schedule time,
 * then post the result via the Telegram Bot API (if TG_BOT_TOKEN +
 * TG_CHAT were captured and job.silent isn't set).
 *
 * No dependencies beyond node + a `pi` binary on PATH.
 * Lockfile: <dir>/daemon.pid — one daemon per job dir.
 */

import {
	existsSync,
	mkdirSync,
	readFileSync,
	readdirSync,
	writeFileSync,
	unlinkSync,
	appendFileSync,
} from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { spawn } from "node:child_process";

const DIR =
	process.env.PI_CRON_DIR ??
	join(homedir(), ".local/state/telegram-agent/cron");
const JOBS = join(DIR, "jobs");
const LOG = join(DIR, "daemon.log");
const PIDFILE = join(DIR, "daemon.pid");
const CATCHUP_GRACE_S = 900;
const SCAN_MS = 30_000;
const RUN_TIMEOUT_MS = 15 * 60 * 1000;

const running = new Set();

function log(...args) {
	try {
		appendFileSync(
			LOG,
			`${new Date().toISOString()} ${args.join(" ")}\n`,
		);
	} catch {}
}

// ---------- minimal cron parser (5-field, dom/dow OR rule) ----------

function fieldMatches(field, val) {
	for (const part of field.split(",")) {
		const p = part.trim();
		if (p === "*") return true;
		if (p.startsWith("*/")) {
			const step = parseInt(p.slice(2));
			if (step && val % step === 0) return true;
			continue;
		}
		if (p.includes("-")) {
			const [a, b] = p.split("-").map(Number);
			if (val >= a && val <= b) return true;
			continue;
		}
		if (parseInt(p) === val) return true;
	}
	return false;
}

const MONTHS = { jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12 };
const DOWS = { sun:0,mon:1,tue:2,wed:3,thu:4,fri:5,sat:6 };

function normField(f, names) {
	return f
		.split(",")
		.map((p) => {
			const m = p.match(/^([a-z]{3})(?:-([a-z]{3}))?$/i);
			if (m && names) {
				const a = names[m[1].toLowerCase()];
				const b = m[2] ? names[m[2].toLowerCase()] : undefined;
				if (a !== undefined)
					return b !== undefined ? `${a}-${b}` : String(a);
			}
			return p;
		})
		.join(",");
}

function cronNext(expr, afterTs) {
	const f = expr.trim().split(/\s+/);
	if (f.length !== 5) return null;
	f[3] = normField(f[3], MONTHS);
	f[4] = normField(f[4], DOWS);
	const domStar = f[2] === "*";
	const dowStar = f[4] === "*";
	let t = new Date(afterTs * 1000);
	t.setSeconds(0, 0);
	t = new Date(t.getTime() + 60_000);
	for (let i = 0; i < 366 * 24 * 60; i++) {
		const dow = t.getDay();
		const domOk = fieldMatches(f[2], t.getDate());
		const dowOk = fieldMatches(f[4], dow);
		// cron rule: when BOTH dom and dow are restricted → OR; else AND
		const dayOk =
			!domStar && !dowStar ? domOk || dowOk : domOk && dowOk;
		if (
			fieldMatches(f[0], t.getMinutes()) &&
			fieldMatches(f[1], t.getHours()) &&
			dayOk &&
			fieldMatches(f[3], t.getMonth() + 1)
		) {
			return Math.floor(t.getTime() / 1000);
		}
		t = new Date(t.getTime() + 60_000);
	}
	return null;
}

function nextRun(spec, after = Math.floor(Date.now() / 1000)) {
	const [kind, arg] = spec.split(/:(.*)/s);
	if (kind === "every") {
		const m = parseInt(arg);
		return m > 0 ? after + m * 60 : null;
	}
	if (kind === "daily") {
		const [h, m] = arg.split(":").map(Number);
		const d = new Date();
		d.setHours(h, m, 0, 0);
		let ts = Math.floor(d.getTime() / 1000);
		if (ts <= after) ts += 86400;
		return ts;
	}
	if (kind === "once") {
		const ts = parseInt(arg);
		return ts > after - CATCHUP_GRACE_S ? ts : null;
	}
	if (kind === "cron") return cronNext(arg, after);
	return null;
}

// ---------- job firing ----------

async function postTelegram(env, text) {
	const tok = env.TG_BOT_TOKEN;
	const chat = env.TG_CHAT;
	if (!tok || !chat) return;
	const body = {
		chat_id: parseInt(chat),
		text: text.slice(0, 4000),
		disable_notification: false,
	};
	if (env.TG_THREAD) body.message_thread_id = parseInt(env.TG_THREAD);
	try {
		await fetch(`https://api.telegram.org/bot${tok}/sendMessage`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify(body),
		});
	} catch (e) {
		log("telegram post failed:", e.message);
	}
}

function runPi(job) {
	return new Promise((resolve) => {
		const args = ["-p", job.prompt];
		const proc = spawn("pi", args, {
			cwd: job.cwd || homedir(),
			env: { ...process.env, ...(job.env || {}) },
			timeout: RUN_TIMEOUT_MS,
		});
		let out = "";
		proc.stdout.on("data", (d) => (out += d));
		proc.stderr.on("data", (d) => log("pi stderr:", String(d).slice(0, 300)));
		proc.on("close", (code) => resolve({ code, out: out.trim() }));
		proc.on("error", (e) => resolve({ code: -1, out: "", err: e.message }));
	});
}

async function fire(job, file) {
	log("firing", job.id, job.spec, `soul=${job.soul}`);
	const { code, out, err } = await runPi(job);
	job.runs = (job.runs || 0) + 1;
	job.last_run = Math.floor(Date.now() / 1000);
	if (code !== 0) {
		job.fails = (job.fails || 0) + 1;
		log("job failed:", job.id, err || `exit ${code}`);
	}
	// post result unless silent or 'none'
	const text = (out || "").trim();
	if (!job.silent && text && !/^(none|-)$/i.test(text)) {
		await postTelegram(job.env || {}, text);
	}
	// reschedule or delete
	if (job.spec.startsWith("once:")) {
		unlinkSync(file);
		return;
	}
	const nxt = nextRun(job.spec);
	if (nxt === null) {
		unlinkSync(file);
		return;
	}
	job.next_run = nxt;
	writeFileSync(file, JSON.stringify(job, null, 2));
}

// ---------- main loop ----------

function scan() {
	if (!existsSync(JOBS)) return;
	const now = Math.floor(Date.now() / 1000);
	for (const name of readdirSync(JOBS)) {
		if (!name.endsWith(".json")) continue;
		const file = join(JOBS, name);
		let job;
		try {
			job = JSON.parse(readFileSync(file, "utf-8"));
		} catch {
			unlinkSync(file);
			continue;
		}
		const jid = job.id || name;
		if (job.paused || running.has(jid)) continue;
		let nxt = job.next_run || 0;
		if (nxt <= 0) {
			nxt = nextRun(job.spec) ?? now + 3600;
			job.next_run = nxt;
			writeFileSync(file, JSON.stringify(job));
			continue;
		}
		if (nxt <= now) {
			if (
				now - nxt > CATCHUP_GRACE_S &&
				!job.spec.startsWith("once:")
			) {
				job.next_run = nextRun(job.spec, now) ?? now + 3600;
				writeFileSync(file, JSON.stringify(job));
				continue;
			}
			running.add(jid);
			fire(job, file).finally(() => running.delete(jid));
		}
	}
}

// single-instance guard
mkdirSync(DIR, { recursive: true });
if (existsSync(PIDFILE)) {
	try {
		const pid = parseInt(readFileSync(PIDFILE, "utf-8"));
		process.kill(pid, 0);
		log("already running as pid", pid, "— exiting");
		process.exit(0);
	} catch {
		/* stale pidfile */
	}
}
writeFileSync(PIDFILE, String(process.pid));
process.on("exit", () => {
	try {
		unlinkSync(PIDFILE);
	} catch {}
});
log("pi-cron daemon up, pid", process.pid, "dir", JOBS);
scan();
setInterval(scan, SCAN_MS);
