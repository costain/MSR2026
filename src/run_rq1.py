#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np
import yaml


# -------------------------
# Config + DB
# -------------------------
def load_config(config_path: str) -> tuple[dict, Path]:
    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p.resolve()}")

    with open(p, "r") as f:
        cfg = yaml.safe_load(f)

    if "data_dir" not in cfg:
        raise KeyError("config.yaml must include: data_dir")

    data_dir = Path(cfg["data_dir"])
    if not data_dir.exists():
        raise FileNotFoundError(f"data_dir not found: {data_dir.resolve()}")

    return cfg, data_dir


def connect_best_practice() -> duckdb.DuckDBPyConnection:
    # Clean, reproducible database every run
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute("SET enable_progress_bar=false;")
    return con


def create_views_from_parquet(con: duckdb.DuckDBPyConnection, data_dir: Path) -> dict:
    """
    Creates DuckDB views pointing at parquet files in data_dir.
    Required: pull_request.parquet
    Optional: repository.parquet, pr_task_type.parquet, revert tables, etc.
    """
    required = ["pull_request"]
    for t in required:
        fp = data_dir / f"{t}.parquet"
        if not fp.exists():
            raise FileNotFoundError(f"Missing required parquet: {fp.resolve()}")

    # Common AIDev tables; only bind if the parquet exists
    candidates = [
        "pull_request", "repository", "user", "pr_task_type",
        "pr_commits", "pr_commit_details", "pr_reviews", "pr_timeline",
        # Optional exploratory revert tables (if present in your local copy)
        "first_revert", "comment_revert", "tl_revert",
    ]

    bound = {}
    for t in candidates:
        fp = data_dir / f"{t}.parquet"
        if fp.exists():
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{fp.as_posix()}');")
            bound[t] = fp.as_posix()

    return bound


# -------------------------
# Stats helpers
# -------------------------
def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    """Wilson score interval for a proportion."""
    if n == 0:
        return (np.nan, np.nan, np.nan)
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = (z * np.sqrt((p * (1 - p) + z * z / (4 * n)) / n)) / denom
    return (p, center - half, center + half)


def add_wilson_ci(df: pd.DataFrame, k_col: str, n_col: str, prefix: str) -> pd.DataFrame:
    lows, highs = [], []
    for _, r in df.iterrows():
        k = int(r[k_col])
        n = int(r[n_col])
        _, lo, hi = wilson_ci(k, n)
        lows.append(lo)
        highs.append(hi)
    df[f"{prefix}_ci_low"] = lows
    df[f"{prefix}_ci_high"] = highs
    return df


# -------------------------
# RQ1 Core SQL
# -------------------------
RQ1_LABELED_SQL = r"""
WITH pr AS (
  SELECT
    id        AS pr_id,
    agent     AS agent,
    repo_id   AS repo_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(closed_at AS TIMESTAMP)  AS closed_at,
    CAST(merged_at AS TIMESTAMP)  AS merged_at,
    CASE
      WHEN merged_at IS NOT NULL THEN 'merged'
      WHEN closed_at IS NOT NULL AND merged_at IS NULL THEN 'closed_unmerged'
      ELSE 'open'
    END AS outcome,
    CASE WHEN merged_at IS NOT NULL THEN 1 ELSE 0 END AS merged,
    CASE WHEN closed_at IS NOT NULL AND merged_at IS NULL THEN 1 ELSE 0 END AS closed_unmerged,
    CASE WHEN closed_at IS NULL AND merged_at IS NULL THEN 1 ELSE 0 END AS open
  FROM pull_request
)
SELECT * FROM pr;
"""

RQ1_OVERALL_SQL = r"""
SELECT
  COUNT(*) AS n_total,
  SUM(merged) AS n_merged,
  SUM(closed_unmerged) AS n_closed_unmerged,
  SUM(open) AS n_open,
  AVG(merged)::DOUBLE AS share_merged,
  AVG(closed_unmerged)::DOUBLE AS share_closed_unmerged,
  AVG(open)::DOUBLE AS share_open
FROM rq1_labeled;
"""

RQ1_BY_AGENT_SQL = r"""
SELECT
  agent,
  COUNT(*) AS n_total,
  SUM(merged) AS n_merged,
  SUM(closed_unmerged) AS n_closed_unmerged,
  SUM(open) AS n_open,
  AVG(merged)::DOUBLE AS share_merged,
  AVG(closed_unmerged)::DOUBLE AS share_closed_unmerged,
  AVG(open)::DOUBLE AS share_open
FROM rq1_labeled
GROUP BY agent
ORDER BY n_total DESC;
"""

RQ1_TIME_TO_DECISION_SQL = r"""
WITH resolved AS (
  SELECT
    pr_id,
    agent,
    CAST(created_at AS TIMESTAMP) AS created_ts,
    CAST(COALESCE(merged_at, closed_at) AS TIMESTAMP) AS decided_ts,
    merged
  FROM rq1_labeled
  WHERE outcome IN ('merged','closed_unmerged')
)
SELECT
  agent,
  COUNT(*) AS n_resolved,
  AVG(DATEDIFF('hour', created_ts, decided_ts))::DOUBLE AS avg_hours_to_decision,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('hour', created_ts, decided_ts))::DOUBLE AS med_hours_to_decision,
  AVG(merged)::DOUBLE AS share_merged_among_resolved
FROM resolved
WHERE created_ts IS NOT NULL AND decided_ts IS NOT NULL
GROUP BY agent
ORDER BY n_resolved DESC;
"""



# -------------------------
# Exploratory reverts (brief; separate file)
# -------------------------
def view_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1;")
        return True
    except Exception:
        return False


def compute_reverts_exploratory(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Optional exploratory summary: among merged PRs, fraction with any "revert signal"
    from (first_revert/comment_revert/tl_revert) tables if they exist.
    Not part of RQ1.
    """
    parts = []
    for t in ["first_revert", "comment_revert", "tl_revert"]:
        if view_exists(con, t):
            parts.append(f"SELECT DISTINCT pr_id FROM {t}")

    if not parts:
        return pd.DataFrame([{"note": "No revert tables present; exploratory analysis skipped."}])

    union_sql = " UNION ".join(parts)

    sql = f"""
    WITH reverted AS (
      {union_sql}
    ),
    merged_pr AS (
      SELECT pr_id, agent
      FROM rq1_labeled
      WHERE outcome='merged'
    )
    SELECT
      m.agent,
      COUNT(*) AS n_merged,
      SUM(CASE WHEN r.pr_id IS NOT NULL THEN 1 ELSE 0 END) AS n_with_revert_signal,
      AVG(CASE WHEN r.pr_id IS NOT NULL THEN 1 ELSE 0 END)::DOUBLE AS share_with_revert_signal
    FROM merged_pr m
    LEFT JOIN reverted r ON r.pr_id = m.pr_id
    GROUP BY m.agent
    ORDER BY n_merged DESC;
    """
    return con.execute(sql).df()


# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml", help="Path to config.yaml (must include data_dir)")
    ap.add_argument("--out_dir", default="out/rq1", help="Output directory")
    ap.add_argument("--write_exploratory_reverts", action="store_true",
                    help="If set, writes rq1_reverts_exploratory.csv (only if revert tables exist)")
    args = ap.parse_args()

    cfg, data_dir = load_config(args.config)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = connect_best_practice()
    bound = create_views_from_parquet(con, data_dir)

    # RQ1 labeled
    con.execute(f"CREATE OR REPLACE VIEW rq1_labeled AS {RQ1_LABELED_SQL}")

    labeled = con.execute("SELECT * FROM rq1_labeled").df()
    overall = con.execute(RQ1_OVERALL_SQL).df()
    by_agent = con.execute(RQ1_BY_AGENT_SQL).df()
    decision = con.execute(RQ1_TIME_TO_DECISION_SQL).df()

    # Add Wilson CIs for merged share (overall + by agent)
    overall = add_wilson_ci(overall, k_col="n_merged", n_col="n_total", prefix="merged")
    by_agent = add_wilson_ci(by_agent, k_col="n_merged", n_col="n_total", prefix="merged")

    # Write outputs
    labeled.to_csv(out_dir / "rq1_labeled.csv", index=False)
    overall.to_csv(out_dir / "rq1_overall.csv", index=False)
    by_agent.to_csv(out_dir / "rq1_by_agent.csv", index=False)
    decision.to_csv(out_dir / "rq1_time_to_decision_by_agent.csv", index=False)

    # Optional exploratory reverts
    if args.write_exploratory_reverts:
        rev = compute_reverts_exploratory(con)
        rev.to_csv(out_dir / "rq1_reverts_exploratory.csv", index=False)

    # Report for reproducibility
    report = {
        "config": {"data_dir": str(cfg["data_dir"]), "db_path": str(cfg.get("db_path", ""))},
        "duckdb_db": ":memory:",
        "parquet_views_bound": bound,
        "n_total_pr": int(len(labeled)),
        "n_agents": int(by_agent.shape[0]),
        "notes": [
            "RQ1 outcomes derived from merged_at/closed_at timestamps (state not used).",
            "Reverts are excluded from RQ1; optional exploratory summary is separate."
        ],
    }
    with open(out_dir / "rq1_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(f"✓ RQ1 done. Outputs in: {out_dir.resolve()}")
    print("  - rq1_overall.csv")
    print("  - rq1_by_agent.csv")
    print("  - rq1_time_to_decision_by_agent.csv")
    print("  - rq1_labeled.csv")
    if args.write_exploratory_reverts:
        print("  - rq1_reverts_exploratory.csv (exploratory only, if available)")
    print("  - rq1_report.json")


if __name__ == "__main__":
    main()
