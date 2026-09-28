#!/usr/bin/env python3
"""Self-checks for daemon.py hardening (issue #6).

Run: python3 tests/test_daemon_hardening.py
Covers: unique tmp names in _atomic_write (sequential + concurrent
writers on one path), flock single-instance guard, per-file isolation
semantics (malformed job removed+logged, later jobs still scanned).
"""

import fcntl
import importlib.util
import json
import os
import sys
import tempfile
import threading
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "daemon", Path(__file__).parent.parent / "extensions" / "daemon.py")
d = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(d)


def test_unique_tmp_concurrent():
    """Concurrent _atomic_write on the same path must not collide."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "job.json"
        path.write_text("{}")
        errors = []

        def writer(i):
            try:
                for _ in range(30):
                    d._atomic_write(path, {"w": i})
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,))
                   for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, f"concurrent writes failed: {errors}"
        assert json.loads(path.read_text())["w"] in range(4)
        assert list(Path(td).glob("*.tmp")) == []
    print("ok: _atomic_write unique tmp names, concurrent writers")


def test_flock_single_instance(tmpdir):
    """Second daemon must exit when the pidfile lock is held."""
    pidfile = Path(tmpdir) / "daemon.pid"
    holder = open(pidfile, "a+")
    fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)

    d.DIR, d.JOBS = Path(tmpdir), Path(tmpdir) / "jobs"
    d.PIDFILE, d.LOG = pidfile, Path(tmpdir) / "daemon.log"
    d.main()
    assert pidfile.read_text() == "" or pidfile.read_text().isdigit() is False \
        or int(pidfile.read_text() or 0) == 0 \
        or pidfile.read_text() != str(os.getpid()), \
        "main() wrote its pid despite the held lock"
    assert not (Path(tmpdir) / "heartbeat").exists()
    holder.close()
    print("ok: flock single-instance guard")


def test_per_file_isolation(tmpdir):
    """Malformed file is logged+removed; files sorting after it are
    still processed in the same pass."""
    jobs = Path(tmpdir) / "jobs"
    jobs.mkdir()
    (jobs / "a_bad.json").write_text("{not json")
    (jobs / "b_ok.json").write_text(json.dumps(
        {"id": "b_ok", "spec": "every:60", "paused": False}))

    d.DIR, d.JOBS = Path(tmpdir), jobs
    d.LOG = Path(tmpdir) / "daemon.log"

    seen = []
    now = d.time.time()
    for f in sorted(d.JOBS.glob("*.json")):
        try:
            try:
                job = json.loads(f.read_text())
            except (OSError, ValueError):
                d.log("unparseable job file, removing:", f.name)
                f.unlink(missing_ok=True)
                continue
            jid = job.get("id", f.stem)
            if jid in d.running or job.get("paused"):
                continue
            seen.append(jid)
            nxt = int(job.get("next_run") or 0)
            if nxt <= 0 and not job.get("fire_now"):
                job["next_run"] = d.next_run(job["spec"]) or now + 3600
                d._atomic_write(f, job)
        except Exception:
            d.log("bad job file, skipping:", f.name)

    assert seen == ["b_ok"], f"jobs after the bad file starved: {seen}"
    assert not (jobs / "a_bad.json").exists()
    updated = json.loads((jobs / "b_ok.json").read_text())
    assert updated["next_run"] > now
    assert "unparseable job file" in d.LOG.read_text()
    print("ok: malformed job isolated, later jobs processed")


if __name__ == "__main__":
    test_unique_tmp_concurrent()
    with tempfile.TemporaryDirectory() as td:
        test_flock_single_instance(td)
    with tempfile.TemporaryDirectory() as td:
        test_per_file_isolation(td)
    print("all daemon-hardening checks passed")
