#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import math
import pandas as pd


def find_url_column(df: pd.DataFrame) -> str | None:
    """Try to find a PR URL column in common variants."""
    candidates = [
        "html_url", "pr_url", "pull_request_url", "url",
        "pr_html_url", "github_url", "link"
    ]
    for c in candidates:
        if c in df.columns:
            return c

    # heuristic: any column name containing 'url'
    url_like = [c for c in df.columns if "url" in c.lower()]
    if url_like:
        return url_like[0]
    return None


def find_pr_id_column(df: pd.DataFrame) -> str | None:
    candidates = ["pr_id", "pull_request_id", "id"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def balanced_sample(df: pd.DataFrame, outcome_col: str, per_outcome: int, seed: int) -> pd.DataFrame:
    """Sample per_outcome rows for each outcome (0/1), if available."""
    out = []
    for val in sorted(df[outcome_col].dropna().unique()):
        sub = df[df[outcome_col] == val]
        if len(sub) == 0:
            continue
        take = min(per_outcome, len(sub))
        out.append(sub.sample(n=take, random_state=seed))
    if not out:
        return df.head(0).copy()
    return pd.concat(out, ignore_index=True)


def df_to_clickable_html(df: pd.DataFrame, out_html: Path, url_col: str | None) -> None:
    df2 = df.copy()
    if url_col and url_col in df2.columns:
        df2["open"] = df2[url_col].apply(
            lambda u: f'<a href="{u}" target="_blank" rel="noopener noreferrer">open</a>'
            if isinstance(u, str) and u.startswith("http") else ""
        )
    else:
        df2["open"] = ""

    # Put key fields first if they exist
    preferred = [
        "bucket", "y_merged", "agent", "repo_id",
        "pr_id", "commit_bucket", "log1p_commits",
        "log1p_delta_loc", "log1p_files", "tests_added",
        "force_push", "has_review", "log1p_hours_to_first_review",
        "open"
    ]
    cols = [c for c in preferred if c in df2.columns]
    # include URL column at end for copy/paste
    if url_col and url_col in df2.columns and url_col not in cols:
        cols.append(url_col)

    df2 = df2[cols]

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Qualitative PR Sample</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #fafafa; }}
    tr:nth-child(even) {{ background: #fcfcfc; }}
    code {{ background: #f3f3f3; padding: 2px 4px; border-radius: 4px; }}
  </style>
</head>
<body>
  <h2>Qualitative PR Sample (from rq2_features.csv)</h2>
  <p>Click <b>open</b> to inspect PRs on GitHub.</p>
  <p><b>Buckets:</b> has_review, force_push (approx: force_push==1 & has_review==1), high_iteration (no force_push, high commits).</p>
  {df2.to_html(index=False, escape=False)}
</body>
</html>"""
    out_html.write_text(html, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Sample PRs for qualitative analysis from rq2_features.csv.")
    ap.add_argument("--features", default="out/rq2/rq2_features.csv", help="Path to RQ2 feature CSV.")
    ap.add_argument("--outdir", default="out/rq2/qual_sample", help="Output directory.")
    ap.add_argument("--seed", type=int, default=42, help="Random seed.")
    ap.add_argument("--min-commits", type=int, default=4, help="Minimum commits for high-iteration bucket.")
    ap.add_argument("--has-review-per-outcome", type=int, default=8, help="Samples per outcome for has_review bucket.")
    ap.add_argument("--force-push-per-outcome", type=int, default=6, help="Samples per outcome for force_push bucket.")
    ap.add_argument("--high-iter-per-outcome", type=int, default=6, help="Samples per outcome for high-iteration bucket.")
    args = ap.parse_args()

    features_path = Path(args.features)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(features_path)

    # Basic required columns (same worldview as run_rq2.py)
    required = ["y_merged", "repo_id", "agent"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns in features CSV: {missing}")

    # Clean & types
    df = df.dropna(subset=["y_merged", "repo_id", "agent"]).copy()
    df["repo_id"] = df["repo_id"].astype(str)
    df["agent"] = df["agent"].astype(str)

    # Detect IDs/URLs if present
    pr_id_col = find_pr_id_column(df)
    url_col = find_url_column(df)

    # If commits are stored as log1p_commits, threshold needs to be in log-space
    if "log1p_commits" not in df.columns:
        raise SystemExit("Expected column 'log1p_commits' in rq2_features.csv (used in Model B).")

    min_log1p_commits = math.log1p(args.min_commits)

    # --- Bucket A: reviewer engagement ---
    if "has_review" not in df.columns:
        raise SystemExit("Expected column 'has_review' in rq2_features.csv.")
    bucket_has_review = df[df["has_review"] == 1].copy()
    bucket_has_review["bucket"] = "has_review"

    samp_has_review = balanced_sample(bucket_has_review, "y_merged", args.has_review_per_outcome, args.seed)

    # --- Bucket B: force push (approx: force_push==1 & has_review==1) ---
    if "force_push" not in df.columns:
        raise SystemExit("Expected column 'force_push' in rq2_features.csv.")
    bucket_force_push = df[(df["force_push"] == 1) & (df["has_review"] == 1)].copy()
    bucket_force_push["bucket"] = "force_push"

    samp_force_push = balanced_sample(bucket_force_push, "y_merged", args.force_push_per_outcome, args.seed + 1)

    # --- Bucket C: high iteration (no force push, high commits) ---
    bucket_high_iter = df[(df["force_push"] == 0) & (df["log1p_commits"] >= min_log1p_commits)].copy()
    bucket_high_iter["bucket"] = "high_iteration"

    samp_high_iter = balanced_sample(bucket_high_iter, "y_merged", args.high_iter_per_outcome, args.seed + 2)

    out = pd.concat([samp_has_review, samp_force_push, samp_high_iter], ignore_index=True)

    # Reorder columns for convenience
    front = ["bucket", "y_merged", "agent", "repo_id"]
    if pr_id_col and pr_id_col not in front:
        front.append(pr_id_col)
    if url_col and url_col not in front:
        front.append(url_col)

    cols = front + [c for c in out.columns if c not in front]
    out = out[cols]

    out_csv = outdir / "qual_sample.csv"
    out_html = outdir / "qual_sample.html"
    out.to_csv(out_csv, index=False)
    df_to_clickable_html(out, out_html, url_col=url_col)

    print(f"✓ Wrote {out_csv}")
    print(f"✓ Wrote {out_html}")
    print("\nCounts by bucket/outcome:")
    print(out.groupby(["bucket", "y_merged"]).size().reset_index(name="n").to_string(index=False))

    if url_col is None:
        print("\nWARNING: No URL column found. Add 'html_url' (preferred) to rq2_features.csv to get clickable links.")


if __name__ == "__main__":
    main()