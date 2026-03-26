#!/usr/bin/env python3
"""Sync harness run results into Turso agent_runs table.

Reads rows from:
  results/<run_name>/pXXiYY/

Expected files per instance:
  _harness/verdict_gen.json
  _harness/verdict_val.json
  _harness/system.log
  _harness/agent.log

This script inserts into `agent_runs` using an idempotent INSERT...WHERE NOT EXISTS.
`agent_run_id` is generated deterministically from run+instance+model+created_at so reruns are safe.
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import json
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path


STEP_RE = re.compile(r"query llm: step (\d+)")
TOTAL_CHANGES_RE = re.compile(r"TOTAL_CHANGES=(\d+)")


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _parse_candidate(system_log: Path) -> dict:
    if not system_log.exists():
        return {}
    try:
        first = system_log.read_text(errors="replace").splitlines()[0]
    except Exception:
        return {}

    if not first.startswith("candidate:"):
        return {}

    raw = first.partition("candidate:")[2].strip()
    try:
        parsed = ast.literal_eval(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_turns(agent_log: Path) -> int | None:
    if not agent_log.exists():
        return None
    max_step: int | None = None
    try:
        for line in agent_log.read_text(errors="replace").splitlines():
            m = STEP_RE.search(line)
            if not m:
                continue
            step = int(m.group(1))
            if max_step is None or step > max_step:
                max_step = step
    except Exception:
        return None
    return None if max_step is None else (max_step + 1)


def _parse_created_at(ts_raw: str | None) -> str | None:
    if not ts_raw:
        return None
    try:
        ts = dt.datetime.fromisoformat(ts_raw)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.isoformat(sep=" ")


def _sql_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value).replace("'", "''")
    return f"'{s}'"


def _build_record(inst_dir: Path, default_run_name: str, tag_mode: str) -> dict | None:
    harness_dir = inst_dir / "_harness"
    verdict_gen = _read_json(harness_dir / "verdict_gen.json")
    verdict_val = _read_json(harness_dir / "verdict_val.json")
    candidate = _parse_candidate(harness_dir / "system.log")

    instance_id = verdict_gen.get("instance_id")
    if not instance_id:
        return None

    run_name = candidate.get("run_name") or default_run_name
    env = candidate.get("env", {}) if isinstance(candidate.get("env"), dict) else {}
    model_name = env.get("MODEL_NAME", "unknown_model")
    resolved_raw = verdict_val.get("resolved")
    resolved = None if resolved_raw is None else bool(resolved_raw)
    turns = _parse_turns(harness_dir / "agent.log")
    created_at = _parse_created_at(verdict_gen.get("ts_end") or verdict_gen.get("ts_begin"))

    if created_at is None:
        mtime = dt.datetime.fromtimestamp(harness_dir.stat().st_mtime, tz=dt.timezone.utc)
        created_at = mtime.isoformat(sep=" ")

    if tag_mode == "run_name":
        tag = run_name
    elif tag_mode == "model_name":
        tag = model_name
    else:
        tag = None

    seed = f"{run_name}|{instance_id}|{model_name}|{created_at}"
    agent_run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, seed))

    return {
        "agent_run_id": agent_run_id,
        "instance_id": instance_id,
        "model_name": model_name,
        "resolved": resolved,
        "turns": turns,
        "tag": tag,
        "created_at": created_at,
        "source_dir": str(inst_dir),
    }


def _collect_records(results_dir: Path, run_name: str | None, tag_mode: str) -> list[dict]:
    run_dirs: list[Path]
    if run_name:
        run_dir = results_dir / run_name
        if not run_dir.is_dir():
            raise FileNotFoundError(f"run not found: {run_dir}")
        run_dirs = [run_dir]
    else:
        run_dirs = sorted([p for p in results_dir.iterdir() if p.is_dir()])

    rows: list[dict] = []
    for rd in run_dirs:
        for inst_dir in sorted([p for p in rd.iterdir() if p.is_dir() and re.match(r"p\d+i\d+", p.name)]):
            rec = _build_record(inst_dir, default_run_name=rd.name, tag_mode=tag_mode)
            if rec is not None:
                rows.append(rec)
    return rows


def _build_sql(rows: list[dict]) -> str:
    stmts = ["BEGIN;"]
    for r in rows:
        rid = _sql_value(r["agent_run_id"])
        stmt = (
            "INSERT INTO agent_runs (agent_run_id, instance_id, model_name, resolved, turns, tag, created_at)\n"
            f"SELECT {rid}, {_sql_value(r['instance_id'])}, {_sql_value(r['model_name'])}, "
            f"{_sql_value(r['resolved'])}, {_sql_value(r['turns'])}, {_sql_value(r['tag'])}, {_sql_value(r['created_at'])}\n"
            f"WHERE NOT EXISTS (SELECT 1 FROM agent_runs WHERE agent_run_id = {rid});"
        )
        stmts.append(stmt)
    stmts.append("COMMIT;")
    stmts.append("SELECT 'TOTAL_CHANGES=' || total_changes();")
    return "\n".join(stmts) + "\n"


def _run_turso_sql(db_name: str, sql: str) -> tuple[int, str, str]:
    proc = subprocess.run(
        ["turso", "db", "shell", db_name],
        input=sql,
        text=True,
        capture_output=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _run_libsql(rows: list[dict], db_url: str, auth_token: str) -> int:
    try:
        import libsql
    except ImportError as e:
        raise RuntimeError("libsql is not installed. Run: python3 -m pip install libsql") from e

    conn = libsql.connect("agent_runs_sync_local.db", sync_url=db_url, auth_token=auth_token)
    conn.sync()

    before = conn.execute("SELECT COUNT(*) FROM agent_runs").fetchone()[0]
    conn.execute("BEGIN")
    try:
        for r in rows:
            conn.execute(
                """
                INSERT INTO agent_runs (
                    agent_run_id, instance_id, model_name, resolved, turns, tag, created_at
                )
                SELECT ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM agent_runs WHERE agent_run_id = ?
                );
                """,
                (
                    r["agent_run_id"],
                    r["instance_id"],
                    r["model_name"],
                    None if r["resolved"] is None else int(bool(r["resolved"])),
                    r["turns"],
                    r["tag"],
                    r["created_at"],
                    r["agent_run_id"],
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    after = conn.execute("SELECT COUNT(*) FROM agent_runs").fetchone()[0]
    return max(0, after - before)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync harness results to Turso agent_runs.")
    parser.add_argument("--results-dir", default="results", help="Harness results directory.")
    parser.add_argument("--run-name", help="Optional single run name under results/.")
    parser.add_argument("--db-name", help="Turso database name for CLI mode (e.g. combined20260326).")
    parser.add_argument("--db-url", help="Turso database URL. Defaults to env TURSO_DATABASE_URL.")
    parser.add_argument("--auth-token", help="Turso auth token. Defaults to env TURSO_AUTH_TOKEN.")
    parser.add_argument(
        "--write-mode",
        choices=["auto", "libsql", "cli"],
        default="auto",
        help="Write using libsql (env/url+token) or Turso CLI.",
    )
    parser.add_argument(
        "--tag-mode",
        choices=["none", "run_name", "model_name"],
        default="none",
        help="How to populate agent_runs.tag.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print summary only, do not write to Turso.")
    args = parser.parse_args()

    results_dir = Path(args.results_dir).resolve()
    if not results_dir.is_dir():
        print(f"results dir not found: {results_dir}", file=sys.stderr)
        return 2

    rows = _collect_records(results_dir, args.run_name, args.tag_mode)
    if not rows:
        print("no rows found to sync")
        return 0

    print(f"discovered rows: {len(rows)}")
    print(f"first source dir: {rows[0]['source_dir']}")

    if args.dry_run:
        for sample in rows[:3]:
            print(
                f"sample: instance_id={sample['instance_id']} model={sample['model_name']} "
                f"resolved={sample['resolved']} turns={sample['turns']} created_at={sample['created_at']}"
            )
        return 0

    db_url = args.db_url or os.getenv("TURSO_DATABASE_URL")
    auth_token = args.auth_token or os.getenv("TURSO_AUTH_TOKEN")

    mode = args.write_mode
    if mode == "auto":
        if db_url and auth_token:
            mode = "libsql"
        elif args.db_name:
            mode = "cli"
        else:
            print(
                "auto mode requires either TURSO_DATABASE_URL+TURSO_AUTH_TOKEN "
                "or --db-name for CLI mode",
                file=sys.stderr,
            )
            return 2

    if mode == "libsql":
        if not db_url or not auth_token:
            print("libsql mode requires db url + auth token (flags or env vars)", file=sys.stderr)
            return 2
        try:
            inserted = _run_libsql(rows, db_url=db_url, auth_token=auth_token)
        except Exception as e:
            print(f"failed to write via libsql: {e}", file=sys.stderr)
            return 1
    else:
        if not args.db_name:
            print("cli mode requires --db-name", file=sys.stderr)
            return 2
        sql = _build_sql(rows)
        code, out, err = _run_turso_sql(args.db_name, sql)
        if code != 0:
            print("failed to write to Turso", file=sys.stderr)
            if out:
                print(out, file=sys.stderr)
            if err:
                print(err, file=sys.stderr)
            return code

        inserted = None
        m = TOTAL_CHANGES_RE.search(out)
        if m:
            inserted = int(m.group(1))

    if inserted is None:
        print("sync completed (insert count unknown)")
    else:
        print(f"sync completed: inserted_or_updated_rows={inserted}, scanned_rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
