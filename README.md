# pi-cron

Self-contained job scheduler for pi agents — schedule work with plain specs, a built-in daemon fires them.

## Specs

| Spec | Meaning |
|---|---|
| `every:90` | every 90 minutes |
| `every:20s` | every 20 seconds |
| `in:30` / `in:30s` | one-shot in 30 minutes / seconds |
| `daily:09:30` | once a day, host-local time |
| `once:1790184000` | one-shot at unix ts |
| `cron:0 9 * * mon-fri` | full 5-field cron (names, ranges, lists, `*/n`) |

## Tools

- `cron_add(spec, prompt, soul?, silent?, times?)` — `times:N` = run at most N times then auto-remove
- `cron_list(all?)` — jobs with next run, stats, flags
- `cron_pause(id, paused?)` — pause/resume
- `cron_edit(id, spec?, prompt?)`
- `cron_remove(id)`

## Daemon

`extensions/daemon.py` — zero-dep python3 loop:

- **install**: `make install` → `pi-cron.service` (systemd user, Restart=always). `make uninstall|status|logs`.
- **auto-start**: `cron_add` starts the systemd unit; without systemd it detached-spawns the daemon.
- jobs: `$PI_CRON_DIR/jobs/*.json` (default `~/.local/state/pi-cron/jobs`)
- env snapshot captured at schedule time (bot tokens, team mailbox, cwd) → jobs run `pi -p --mode json --no-session` as their soul
- delivery: team mailbox `say` when `PI_TEAM_DIR` is set (journaled, posted as the soul), else direct Telegram Bot API
- missed-run catch-up ≤15min, overlap lock, max 4 concurrent runs, atomic job writes, log rotation, heartbeat file
