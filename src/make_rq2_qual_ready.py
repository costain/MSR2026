#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd
import yaml

CONFIG_PATH = Path("config.yaml")
DEFAULT_FEATURES = Path("out/rq2/rq2_features.csv")
DEFAULT_OUTDIR = Path("out/rq2/qual_ready")


def sql_str(p: Path) -> str:
    return str(p).replace("'", "''")


def pick_required(aidev_dir: Path, filename: str) -> Path:
    p = aidev_dir / filename
    if not p.exists():
        raise SystemExit(f"Missing required file: {p}")
    return p


def main():
    if not CONFIG_PATH.exists():
        raise SystemExit(f"config.yaml not found at: {CONFIG_PATH.resolve()}")

    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    data_dir = cfg.get("data_dir")
    if not data_dir:
        raise SystemExit("Missing key 'data_dir' in config.yaml (expected: data_dir: \"AIDev\").")

    aidev_dir = Path(data_dir).expanduser().resolve()
    features_csv = Path(cfg.get("rq2_features", DEFAULT_FEATURES)).expanduser().resolve()
    outdir = Path(cfg.get("qual_output", DEFAULT_OUTDIR)).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    if not features_csv.exists():
        raise SystemExit(f"Features CSV not found: {features_csv}")

    # YOU REQUESTED: use relations repository and pull_request (not all_*)
    pr_parquet = pick_required(aidev_dir, "pull_request.parquet")
    repo_parquet = pick_required(aidev_dir, "repository.parquet")

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")

    con.execute(f"""
        CREATE OR REPLACE VIEW rq2_features AS
        SELECT * FROM read_csv_auto('{sql_str(features_csv)}', HEADER=TRUE);
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW pull_request AS
        SELECT * FROM read_parquet('{sql_str(pr_parquet)}');
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW repository AS
        SELECT * FROM read_parquet('{sql_str(repo_parquet)}');
    """)

    # IMPORTANT: rq2_features has pr_id, not id
    sql = """
    SELECT
      -- identifiers
      f.pr_id,
      f.agent,
      f.y_merged,

      -- qualitative context
      r.full_name AS repo_full_name,
      pr.number,
      pr.title,
      pr.html_url,

      -- state & timing
      pr.state,
      pr.created_at,
      pr.closed_at,
      pr.merged_at,

      -- RQ2 signals (keep what exists in your features CSV)
      f.has_review,
      f.force_push,
      f.commit_bucket,
      f.log1p_commits,
      f.delta_loc,
      f.log1p_delta_loc,
      f.n_files,
      f.log1p_files,
      f.tests_added,
      f.hours_to_first_review,
      f.log1p_hours_to_first_review

    FROM rq2_features f
    LEFT JOIN pull_request pr
      ON pr.id = f.pr_id
    LEFT JOIN repository r
      ON r.id = pr.repo_id
    ;
    """

    df = con.execute(sql).fetchdf()

    # If your features CSV doesn't contain some of these optional columns,
    # DuckDB will error. So we do a safer projection next:
    #
    # We keep only columns that exist after the join.
    wanted = [
        "pr_id", "agent", "y_merged",
        "repo_full_name", "number", "title", "html_url",
        "state", "created_at", "closed_at", "merged_at",
        "has_review", "force_push", "commit_bucket",
        "log1p_commits", "delta_loc", "log1p_delta_loc",
        "n_files", "log1p_files", "tests_added",
        "hours_to_first_review", "log1p_hours_to_first_review"
    ]
    existing = [c for c in wanted if c in df.columns]
    df = df[existing].copy()

    # Fallback: construct PR URL if html_url missing
    if "html_url" in df.columns:
        missing = df["html_url"].isna() | (df["html_url"].astype(str).str.len() == 0)
        if missing.any() and "repo_full_name" in df.columns and "number" in df.columns:
            df.loc[missing, "html_url"] = df.loc[missing].apply(
                lambda r: f"https://github.com/{r['repo_full_name']}/pull/{int(r['number'])}"
                if pd.notna(r.get("repo_full_name")) and pd.notna(r.get("number")) else pd.NA,
                axis=1,
            )

    out_csv = outdir / "rq2_qual_ready.csv"
    out_html = outdir / "rq2_qual_ready.html"

    df.to_csv(out_csv, index=False)

    df_html = df.copy()
    if "html_url" in df_html.columns:
        df_html.insert(
            0,
            "open",
            df_html["html_url"].apply(
                lambda u: f'<a href="{u}" target="_blank" rel="noopener noreferrer">open</a>'
                if isinstance(u, str) and u.startswith("http") else ""
            ),
        )

    html_doc = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>RQ2 Qualitative Ready</title>
<style>
body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
th {{ position: sticky; top: 0; background: #fafafa; }}
tr:nth-child(even) {{ background: #fcfcfc; }}
</style>
</head>
<body>
<h2>RQ2 Qualitative Ready</h2>
<p>Click <b>open</b> to inspect PRs on GitHub.</p>
{df_html.to_html(index=False, escape=False)}
</body>
</html>
"""
    out_html.write_text(html_doc, encoding="utf-8")

    # Quick sanity: how many PR joins failed (missing number)
    if "number" in df.columns:
        n_fail = int(df["number"].isna().sum())
        if n_fail:
            print(f"WARNING: {n_fail} rows did not match pull_request.id (missing PR metadata).")

    print("✓ Done")
    print(f"data_dir:     {aidev_dir}")
    print(f"features_csv: {features_csv}")
    print(f"outdir:       {outdir}")
    print(f"wrote:        {out_csv}")
    print(f"wrote:        {out_html}")
    print(f"rows:         {len(df)}")


if __name__ == "__main__":
    main()