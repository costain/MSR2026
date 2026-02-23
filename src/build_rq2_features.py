#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import yaml
import duckdb
import pandas as pd


def load_config(path: str) -> Path:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    data_dir = Path(cfg["data_dir"])
    if not data_dir.exists():
        raise FileNotFoundError(f"data_dir not found: {data_dir}")
    return data_dir


def connect_clean() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute("SET enable_progress_bar=false;")
    return con


def bind_views(con: duckdb.DuckDBPyConnection, data_dir: Path):
    def bind(name: str):
        fp = data_dir / f"{name}.parquet"
        if fp.exists():
            con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{fp.as_posix()}');")
        else:
            raise FileNotFoundError(f"Missing parquet: {fp}")

    # Required for RQ2
    for t in [
        "pull_request",
        "pr_task_type",
        "pr_commits",
        "pr_commit_details",
        "pr_reviews",
        "pr_timeline",
        "repository",
        "user",
    ]:
        bind(t)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--out", default="out/rq2/rq2_features.csv")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data_dir = load_config(args.config)
    con = connect_clean()
    bind_views(con, data_dir)

    # ---- Core labeled set: agent-authored PRs + outcome (same logic as RQ1) ----
    # We define agent-authored PRs using pr_task_type with non-null agent.
    # Outcome uses timestamps: merged_at, closed_at.
    con.execute(r"""
    CREATE OR REPLACE VIEW rq2_base AS
    WITH agentic AS (
      SELECT DISTINCT
        t.id AS pr_id,
        t.agent AS agent
      FROM pr_task_type t
      WHERE t.agent IS NOT NULL
    ),
    pr AS (
      SELECT
        p.id AS pr_id,
        p.repo_id,
        p.user_id AS author_user_id,
        TRY_CAST(p.created_at AS TIMESTAMP) AS created_ts,
        TRY_CAST(p.closed_at  AS TIMESTAMP) AS closed_ts,
        TRY_CAST(p.merged_at  AS TIMESTAMP) AS merged_ts
      FROM pull_request p
    ),
    labeled AS (
      SELECT
        a.pr_id,
        a.agent,
        pr.repo_id,
        pr.author_user_id,
        pr.created_ts,
        pr.closed_ts,
        pr.merged_ts,
        CASE
          WHEN pr.merged_ts IS NOT NULL THEN 'merged'
          WHEN pr.closed_ts IS NOT NULL AND pr.merged_ts IS NULL THEN 'closed_unmerged'
          ELSE 'open'
        END AS outcome,
        CASE WHEN pr.merged_ts IS NOT NULL THEN 1 ELSE 0 END AS y_merged
      FROM agentic a
      JOIN pr ON pr.pr_id = a.pr_id
    )
    SELECT *
    FROM labeled
    WHERE outcome IN ('merged','closed_unmerged');
    """)

    # ---- Feature blocks ----
    # Commits
    con.execute(r"""
    CREATE OR REPLACE VIEW f_commits AS
    SELECT
      pr_id,
      COUNT(DISTINCT sha) AS n_commits
    FROM pr_commits
    GROUP BY pr_id;
    """)

    # Commit details: LOC + files + tests
    con.execute(r"""
    CREATE OR REPLACE VIEW f_changes AS
    SELECT
      pr_id,
      SUM(COALESCE(additions, 0)) AS add_lines,
      SUM(COALESCE(deletions, 0)) AS del_lines,
      SUM(COALESCE(additions, 0)) + SUM(COALESCE(deletions, 0)) AS delta_loc,
      COUNT(DISTINCT filename) FILTER (WHERE filename IS NOT NULL) AS n_files,
      MAX(
        CASE
          WHEN filename ILIKE '%test%' OR filename ILIKE '%spec%' OR filename ILIKE '%__tests__%'
          THEN 1 ELSE 0
        END
      ) AS tests_added
    FROM pr_commit_details
    GROUP BY pr_id;
    """)

    # Reviews: has_review + time_to_first_review
    con.execute(r"""
    CREATE OR REPLACE VIEW f_reviews AS
    WITH first_review AS (
      SELECT
        pr_id,
        MIN(TRY_CAST(submitted_at AS TIMESTAMP)) AS first_review_ts
      FROM pr_reviews
      GROUP BY pr_id
    )
    SELECT
      b.pr_id,
      CASE WHEN fr.first_review_ts IS NOT NULL THEN 1 ELSE 0 END AS has_review,
      CASE
        WHEN fr.first_review_ts IS NOT NULL AND b.created_ts IS NOT NULL
        THEN DATEDIFF('hour', b.created_ts, fr.first_review_ts)
        ELSE NULL
      END AS hours_to_first_review
    FROM rq2_base b
    LEFT JOIN first_review fr ON fr.pr_id = b.pr_id;
    """)

    # Force-push: use timeline event heuristic
    # AIDev timeline has `event` and sometimes `message`; we match either.
    con.execute(r"""
    CREATE OR REPLACE VIEW f_forcepush AS
    SELECT
      pr_id,
      MAX(
        CASE
          WHEN event ILIKE '%force%' OR COALESCE(message,'') ILIKE '%force%'
          THEN 1 ELSE 0
        END
      ) AS force_push
    FROM pr_timeline
    GROUP BY pr_id;
    """)

    # ---- Assemble feature table ----
    con.execute(r"""
    CREATE OR REPLACE TABLE rq2_features AS
    SELECT
      b.pr_id,
      b.repo_id,
      b.agent,
      b.y_merged,

      COALESCE(c.n_commits, 0) AS n_commits,
      CASE
        WHEN COALESCE(c.n_commits, 0) <= 1 THEN 1
        WHEN c.n_commits = 2 THEN 2
        WHEN c.n_commits = 3 THEN 3
        WHEN c.n_commits = 4 THEN 4
        ELSE 5
      END AS commit_bucket,

      COALESCE(ch.delta_loc, 0) AS delta_loc,
      COALESCE(ch.n_files, 0) AS n_files,
      COALESCE(ch.tests_added, 0) AS tests_added,

      COALESCE(fp.force_push, 0) AS force_push,

      COALESCE(rv.has_review, 0) AS has_review,
      rv.hours_to_first_review AS hours_to_first_review

    FROM rq2_base b
    LEFT JOIN f_commits   c  ON c.pr_id  = b.pr_id
    LEFT JOIN f_changes   ch ON ch.pr_id = b.pr_id
    LEFT JOIN f_forcepush fp ON fp.pr_id = b.pr_id
    LEFT JOIN f_reviews   rv ON rv.pr_id = b.pr_id;
    """)

    df = con.execute("SELECT * FROM rq2_features").df()

    # Derived transforms for modeling stability
    df["log1p_commits"] = (df["n_commits"]).apply(lambda x: __import__("math").log1p(x))
    df["log1p_delta_loc"] = (df["delta_loc"]).apply(lambda x: __import__("math").log1p(x))
    df["log1p_files"] = (df["n_files"]).apply(lambda x: __import__("math").log1p(x))

    # Missingness treatment for time-to-first-review
    # Keep has_review and set missing time to 0 after log1p (standard informative-missingness pattern)
    import numpy as np
    df["log1p_hours_to_first_review"] = np.where(
        df["hours_to_first_review"].notna(),
        np.log1p(df["hours_to_first_review"].clip(lower=0)),
        0.0,
    )

    df.to_csv(out_path, index=False)
    print(f"✓ Wrote features: {out_path}  (n={len(df):,})")


if __name__ == "__main__":
    main()
