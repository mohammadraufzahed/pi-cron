#!/usr/bin/env python3
"""pi-cron daemon — self-contained scheduler for pi-cron jobs.

Spawned by the extension on first cron_add (or via `make install`
→ systemd user unit). Scans $PI_CRON_DIR/jobs every 30s; due jobs run
`pi -p "<prompt>"` with the env snapshot captured at schedule time,
then post the result via the Telegram Bot API when TG_BOT_TOKEN +
TG_CHAT were captured and the job isn't silent.

Zero deps beyond python3 + a `pi` binary on PATH.
Lockfile: <dir>/daemon.pid — one daemon per job dir.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timedelta
from pathlib import Path

DIR = Path(os.environ.get("PI_CRON_DIR") or
           Path.home() / ".local/state/pi-cron")
JOBS = DIR / "jobs"
LOG = DIR / "daemon.log"
PIDFILE = DIR / "daemon.pid"
CATCHUP_GRACE_S = 900
SCAN_S = 30
RUN_TIMEOUT_S = 15 * 60

running: set[str] = set()
sem = threading.BoundedSemaphore(4)  # max concurrent pi runs


def _atomic_write(path: Path, job: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(job))
    tmp.replace(path)


def _rotate_log() -> None:
    try:
        if LOG.exists() and LOG.stat().st_size > 512 * 1024:
            LOG.write_bytes(LOG.read_bytes()[-256 * 1024:])
    except OSError:
        pass


def log(*args):
    try:
        with open(LOG, "a") as f:
            f.write(f"{datetime.now().isoformat()} {' '.join(map(str, args))}\n")
    except OSError:
        pass


# ---------- minimal cron parser (5-field, dom/dow OR rule) ----------

_MONTHS = {m: i + 1 for i, m in enumerate(
    "jan feb mar apr may jun jul aug sep oct nov dec".split())}
_DOWS = {d: i for i, d in enumerate("sun mon tue wed thu fri sat".split())}


def _field_ok(field: str, val: int) -> bool:
    for part in field.split(","):
        p = part.strip()
        if p == "*":
            return True
        if p.startswith("*/"):
            step = int(p[2:] or 0)
            if step and val % step == 0:
                return True
            continue
        if "-" in p:
            a, b = (int(x) for x in p.split("-", 1))
            if a <= val <= b:
                return True
            continue
        try:
            if int(p) == val:
                return True
        except ValueError:
            pass
    return False


def _names(field: str, table: dict[str, int]) -> str:
    out = []
    for p in field.split(","):
        m = p.strip().lower()
        if "-" in m:
            a, _, b = m.partition("-")
            if a in table and b in table:
                out.append(f"{table[a]}-{table[b]}")
                continue
        out.append(str(table[m]) if m in table else p)
    return ",".join(out)


def _split_tz(arg: str) -> tuple[str, object | None]:
    """'<expr>@<IANA zone>' — daily:09:00@Asia/Tehran, cron:0 9 * * 1-5@UTC.
    Returns (expr, tzinfo|None)."""
    if "@" not in arg:
        return arg, None
    expr, _, zone = arg.partition("@")
    try:
        from zoneinfo import ZoneInfo

        return expr, ZoneInfo(zone)
    except Exception:
        log("bad timezone in spec, using host local:", arg)
        return expr, None


def cron_next(expr: str, after: int, tz=None) -> int | None:
    f = expr.split()
    if len(f) != 5:
        return None
    f[3] = _names(f[3], _MONTHS)
    f[4] = _names(f[4], _DOWS)
    dom_star, dow_star = f[2] == "*", f[4] == "*"
    t = datetime.fromtimestamp(after, tz=tz).replace(second=0, microsecond=0)
    t += timedelta(minutes=1)
    for _ in range(366 * 24 * 60):
        dom_ok = _field_ok(f[2], t.day)
        dow_ok = _field_ok(f[4], (t.weekday() + 1) % 7)  # Sun=0
        day_ok = (dom_ok or dow_ok) if not dom_star and not dow_star \
            else dom_ok and dow_ok
        if (_field_ok(f[0], t.minute) and _field_ok(f[1], t.hour)
                and day_ok and _field_ok(f[3], t.month)):
            return int(t.timestamp())
        t += timedelta(minutes=1)
    return None


def next_run(spec: str, after: int | None = None) -> int | None:
    now = after or int(time.time())
    kind, _, arg = spec.partition(":")
    if kind == "every":
        if arg.endswith("s"):
            s = int(arg[:-1] or 0)
            return now + s if s > 0 else None
        m = int(arg or 0)
        return now + m * 60 if m > 0 else None
    if kind == "daily":
        arg, tz = _split_tz(arg)
        try:
            h, m = (int(x) for x in arg.split(":"))
        except ValueError:
            return None
        d = datetime.now(tz=tz).replace(hour=h, minute=m, second=0, microsecond=0)
        if int(d.timestamp()) <= now:
            d += timedelta(days=1)
        return int(d.timestamp())
    if kind == "once":
        ts = int(arg or 0)
        return ts if ts > now - CATCHUP_GRACE_S else None
    if kind == "cron":
        arg, tz = _split_tz(arg)
        return cron_next(arg, now, tz)
    return None


# ---------- firing ----------


def deliver(env: dict, soul: str, text: str) -> None:
    """Post the run result — prefer the team mailbox (host delivers as
    the soul and journals it); fall back to direct Bot API."""
    team_dir = env.get("PI_TEAM_DIR")
    if team_dir:
        try:
            req_dir = Path(team_dir) / "requests"
            req_dir.mkdir(parents=True, exist_ok=True)
            rid = uuid.uuid4().hex
            (req_dir / f"{rid}.json").write_text(json.dumps({
                "id": rid, "from": soul, "to": "host", "kind": "say",
                "text": text[:4000],
                "chat": env.get("PI_TEAM_CHAT") or env.get("TG_CHAT"),
                "thread": env.get("PI_TEAM_THREAD"),
                "at": int(time.time() * 1000),
            }))
            return
        except Exception as e:
            log("mailbox write failed, direct post:", e)
    tok, chat = env.get("TG_BOT_TOKEN"), env.get("TG_CHAT")
    if not tok or not chat:
        return
    body = {"chat_id": int(chat), "text": text[:4000]}
    if env.get("TG_THREAD"):
        body["message_thread_id"] = int(env["TG_THREAD"])
    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            data=json.dumps(body).encode(),
            headers={"content-type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        log("telegram post failed:", e)


def _final_text(stdout: str) -> str:
    """Last assistant text from pi's --mode json event stream."""
    text = ""
    for line in stdout.splitlines():
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        msg = ev.get("message") or {}
        if (ev.get("type") == "message_end"
                and msg.get("role") == "assistant"):
            parts = [p.get("text", "") for p in msg.get("content", [])
                     if isinstance(p, dict) and p.get("type") == "text"]
            if parts:
                text = "".join(parts)
    return text


def fire(job: dict, path: Path) -> None:
    log("firing", job["id"], job["spec"], f"soul={job.get('soul')}")
    env = {**os.environ, **(job.get("env") or {})}
    # jobs snapshot GH_TOKEN at create-time — it expires within the
    # hour. The gh shim re-mints fresh per call; drop the stale one.
    env.pop("GH_TOKEN", None)
    repo = env.get("PI_TEAM_REPO")
    if repo:
        env["PATH"] = f"{repo}/tools/bin:{env.get('PATH', '')}"
    args = ["pi", "-p", job["prompt"], "--mode", "json", "--no-session"]
    if job.get("tools"):
        args += ["--tools", job["tools"]]
    # extension (monitoring/team tools) — job may pin one, else the
    # default from env so cron runs get the same tools as chats
    ext = job.get("extension") or os.environ.get("PI_EXTENSION", "")
    if ext:
        args += ["--extension", ext]
    try:
        r = subprocess.run(
            args,
            cwd=job.get("cwd") or str(Path.home()),
            env=env, capture_output=True, text=True,
            timeout=RUN_TIMEOUT_S,
        )
        out = _final_text(r.stdout or "") or (r.stdout or "").strip()
        job["last_status"] = "ok" if r.returncode == 0 else "failed"
        if r.returncode != 0:
            job["fails"] = int(job.get("fails") or 0) + 1
            log("job failed:", job["id"], "exit", r.returncode,
                (r.stderr or "")[:200])
    except subprocess.TimeoutExpired:
        out, job["fails"] = "", int(job.get("fails") or 0) + 1
        job["last_status"] = "timeout"
        log("job timed out:", job["id"])
    except Exception as e:
        out, job["fails"] = "", int(job.get("fails") or 0) + 1
        job["last_status"] = "error"
        log("job error:", job["id"], e)

    job["runs"] = int(job.get("runs") or 0) + 1
    job["last_run"] = int(time.time())
    job["last_answer"] = (out or "")[:200]
    if not job.get("silent") and out and out.lower() not in ("none", "-"):
        deliver(job.get("env") or {}, job.get("soul", ""), out)

    times = int(job.get("times") or 0)
    if job["spec"].startswith("once:") or (times and job["runs"] >= times):
        path.unlink(missing_ok=True)
        log("job done:", job["id"], f"({job['runs']} runs)")
        return
    nxt = next_run(job["spec"])
    if nxt is None:
        path.unlink(missing_ok=True)
        return
    job["next_run"] = nxt
    _atomic_write(path, job)


# ---------- loop ----------


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def main() -> None:
    JOBS.mkdir(parents=True, exist_ok=True)
    if PIDFILE.exists():
        try:
            if _pid_alive(int(PIDFILE.read_text())):
                log("already running — exit")
                return
        except (ValueError, OSError):
            pass
    PIDFILE.write_text(str(os.getpid()))
    log("pi-cron daemon up, pid", os.getpid(), "dir", JOBS)

    def _bye(*_):
        PIDFILE.unlink(missing_ok=True)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _bye)
    signal.signal(signal.SIGINT, _bye)

    while True:
        try:
            _rotate_log()
            (DIR / "heartbeat").write_text(str(int(time.time())))
            now = int(time.time())
            for f in sorted(JOBS.glob("*.json")):
                try:
                    job = json.loads(f.read_text())
                except (OSError, ValueError):
                    f.unlink(missing_ok=True)
                    continue
                jid = job.get("id", f.stem)
                if jid in running or job.get("paused"):
                    continue
                nxt = int(job.get("next_run") or 0)
                if nxt <= 0:
                    job["next_run"] = next_run(job["spec"]) or now + 3600
                    _atomic_write(f, job)
                    continue
                if nxt <= now:
                    if (now - nxt > CATCHUP_GRACE_S
                            and not job["spec"].startswith("once:")):
                        job["next_run"] = next_run(job["spec"], now) \
                            or now + 3600
                        _atomic_write(f, job)
                        continue
                    if not sem.acquire(blocking=False):
                        continue  # at capacity — retry next scan
                    running.add(jid)

                    def _go(j=job, p=f, i=jid):
                        try:
                            fire(j, p)
                        except Exception:
                            log("fire crashed:", i)
                        finally:
                            running.discard(i)
                            sem.release()

                    threading.Thread(target=_go, daemon=True).start()
        except Exception:
            log("scan loop error")
        time.sleep(SCAN_S)


if __name__ == "__main__":
    main()
