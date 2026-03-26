#!/usr/bin/env python3
"""Update harness/projects.json with the next batch of instance_ids from Turso.

Reads from Turso table:
  instances(repo, instance_id, base_commit_date, ...)

Selection logic:
  1) Filter by --repo
  2) Order by base_commit_date ASC, then instance_id ASC
  3) If --after-instance-id is given, find it in that ordered list
  4) Take the next N instance_ids (default N=10)
     If --after-instance-id is omitted, start from the first instance.

Automation:
  - Saves last run metadata to a state file (default: .set_projects_next_batch_state.json)
  - `--resume` can reuse prior repo + anchor (last selected instance_id) + batch_size
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = SCRIPT_DIR / ".set_projects_next_batch_state.json"
LOCAL_REPLICA_PATH = SCRIPT_DIR / ".set_projects_next_batch.db"


def load_instances_for_repo(conn, repo: str) -> list[tuple[str, str | None]]:
    rows = conn.execute(
        """
        SELECT instance_id, base_commit_date
        FROM instances
        WHERE repo = ?
        ORDER BY
          CASE WHEN base_commit_date IS NULL OR base_commit_date = '' THEN 1 ELSE 0 END ASC,
          base_commit_date ASC,
          instance_id ASC
        """,
        (repo,),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def build_projects_payload(instance_ids: list[str], layout: str) -> list[list[str]]:
    if layout == "one_per_project":
        return [[iid] for iid in instance_ids]
    return [instance_ids]


def _load_state(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"state file not found: {path} (run once without --resume first)")
    try:
        raw = json.loads(path.read_text())
    except Exception as e:
        raise SystemExit(f"failed to parse state file {path}: {e}") from e
    if not isinstance(raw, dict):
        raise SystemExit(f"invalid state file format: {path}")
    return raw


def _save_state(
    state_path: Path,
    *,
    repo: str,
    batch_size: int,
    anchor_instance_id: str | None,
    selected_ids: list[str],
    layout: str,
    projects_path: Path,
) -> None:
    payload = {
        "repo": repo,
        "batch_size": batch_size,
        "after_instance_id": anchor_instance_id,
        "last_instance_id": selected_ids[-1],
        "first_instance_id": selected_ids[0],
        "selected_count": len(selected_ids),
        "selected_instance_ids": selected_ids,
        "layout": layout,
        "projects_path": str(projects_path),
        "updated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    state_path.write_text(json.dumps(payload, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Set harness/projects.json to next batch from Turso instances.")
    parser.add_argument("--repo", help="Repo name in instances table (e.g. element-hq/element-web).")
    parser.add_argument(
        "--after-instance-id",
        help=(
            "Anchor instance_id. Script selects rows after this id in base_commit_date order. "
            "If omitted, starts from the first instance."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of next instance_ids to select (default: 10, or previous value with --resume).",
    )
    parser.add_argument(
        "--layout",
        choices=["single_project", "one_per_project"],
        default="single_project",
        help="projects.json layout. single_project -> [[id1,id2,...]], one_per_project -> [[id1],[id2],...].",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse state from previous run: repo + last_instance_id + batch_size (unless overridden).",
    )
    parser.add_argument(
        "--state-path",
        default=str(DEFAULT_STATE_PATH),
        help=f"Path for persisted state (default: {DEFAULT_STATE_PATH}).",
    )
    parser.add_argument("--projects-path", default="projects.json", help="Path to projects.json (default: projects.json).")
    parser.add_argument("--no-save-state", action="store_true", help="Do not write state file after success.")
    parser.add_argument("--no-backup", action="store_true", help="Do not create timestamped backup before writing.")
    parser.add_argument("--dry-run", action="store_true", help="Print selected ids only; do not write file.")
    args = parser.parse_args()

    if args.batch_size is not None and args.batch_size <= 0:
        raise SystemExit("batch-size must be > 0")

    state_path = Path(args.state_path).resolve()
    prior_state: dict = {}
    if args.resume:
        prior_state = _load_state(state_path)

    repo = args.repo or prior_state.get("repo")
    anchor_instance_id = (
        args.after_instance_id
        or prior_state.get("last_instance_id")
        or prior_state.get("after_instance_id")
    )
    batch_size = args.batch_size
    if batch_size is None:
        prior_batch = prior_state.get("batch_size")
        if isinstance(prior_batch, int) and prior_batch > 0:
            batch_size = prior_batch
        else:
            batch_size = 10

    if not repo:
        raise SystemExit("Missing --repo (or provide --resume with saved state).")

    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None
    if load_dotenv is not None:
        load_dotenv()
    db_url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    if not db_url or not auth_token:
        raise SystemExit("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN in environment/.env")

    try:
        import libsql
    except ImportError as e:
        raise SystemExit("libsql not installed. Run: python3 -m pip install libsql") from e

    conn = libsql.connect(str(LOCAL_REPLICA_PATH), sync_url=db_url, auth_token=auth_token)
    conn.sync()

    rows = load_instances_for_repo(conn, repo)
    if not rows:
        raise SystemExit(f"No instances found for repo: {repo}")

    ordered_ids = [iid for iid, _ in rows]
    if anchor_instance_id:
        try:
            anchor_idx = ordered_ids.index(anchor_instance_id)
        except ValueError:
            raise SystemExit(
                f"after-instance-id not found within repo ordering: {anchor_instance_id}\n"
                f"repo={repo}, total_instances={len(ordered_ids)}"
            )
    else:
        anchor_idx = -1

    start = anchor_idx + 1
    end = start + batch_size
    batch = ordered_ids[start:end]

    if not batch:
        raise SystemExit(
            f"No next instances after anchor.\n"
            f"anchor_index={anchor_idx}, total_instances={len(ordered_ids)}"
        )

    print(f"repo={repo}")
    print(f"anchor={anchor_instance_id or '<START>'}")
    print(f"selected_count={len(batch)} (requested={batch_size})")
    print(f"next_anchor={batch[-1]}")
    for iid in batch:
        print(iid)

    if args.dry_run:
        return 0

    projects_path = Path(args.projects_path).resolve()
    projects_path.parent.mkdir(parents=True, exist_ok=True)

    if projects_path.exists() and not args.no_backup:
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = projects_path.with_name(f"{projects_path.stem}.backup.{ts}{projects_path.suffix}")
        backup_path.write_text(projects_path.read_text())
        print(f"backup_written={backup_path}")

    payload = build_projects_payload(batch, args.layout)
    projects_path.write_text(json.dumps(payload, indent=4) + "\n")
    print(f"projects_updated={projects_path}")

    if not args.no_save_state:
        _save_state(
            state_path,
            repo=repo,
            batch_size=batch_size,
            anchor_instance_id=anchor_instance_id,
            selected_ids=batch,
            layout=args.layout,
            projects_path=projects_path,
        )
        print(f"state_updated={state_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
