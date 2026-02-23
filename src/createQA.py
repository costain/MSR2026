import pandas as pd
import numpy as np
from pathlib import Path

# ======================
# Configuration
# ======================
INP = "rq2_qual_ready.csv"
OUT_CSV = "qual_sample_60.csv"
OUT_HTML = "qual_sample_60.html"
OUT_CODING = "coding_sheet_60.csv"
SEED = 42


# ======================
# Load data
# ======================
df = pd.read_csv(INP)


def balanced_sample(d: pd.DataFrame, per_outcome: int, seed: int) -> pd.DataFrame:
    """Sample per_outcome merged and unmerged PRs if available."""
    out = []
    for y in [1, 0]:
        sub = d[d["y_merged"] == y]
        n = min(per_outcome, len(sub))
        if n > 0:
            out.append(sub.sample(n=n, random_state=seed))
    return pd.concat(out, ignore_index=True) if out else d.head(0).copy()


# ======================
# Determine high-iteration PRs
# ======================
if "commit_bucket" in df.columns:
    high_commit = df["commit_bucket"] >= 4
elif "log1p_commits" in df.columns:
    high_commit = np.expm1(df["log1p_commits"]).round() >= 4
else:
    raise SystemExit("Need commit_bucket or log1p_commits to identify high-iteration PRs")


# ======================
# Bucket 1: Has review
# ======================
b1 = df[df["has_review"] == 1].copy()
b1["bucket"] = "has_review"
s1 = balanced_sample(b1, per_outcome=10, seed=SEED)


# ======================
# Bucket 2: Force push after review
# ======================
b2 = df[(df["has_review"] == 1) & (df["force_push"] == 1)].copy()
b2["bucket"] = "force_push"
s2 = balanced_sample(b2, per_outcome=10, seed=SEED + 1)


# ======================
# Bucket 3: High iteration, no force push
# ======================
b3 = df[(df["force_push"] == 0) & (high_commit)].copy()
b3["bucket"] = "high_iteration"
s3 = balanced_sample(b3, per_outcome=10, seed=SEED + 2)


# ======================
# Combine sample
# ======================
sample = pd.concat([s1, s2, s3], ignore_index=True)


# ======================
# Keep only fields needed for qualitative coding
# ======================
keep = [
    "bucket", "y_merged", "agent",
    "repo_full_name", "number", "title", "html_url",
    "pr_id", "has_review", "force_push"
]
sample = sample[[c for c in keep if c in sample.columns]]


# ======================
# Write sample CSV
# ======================
sample.to_csv(OUT_CSV, index=False)


# ======================
# Write coding sheet CSV (adds 5 empty columns)
# ======================
coding = sample.copy()
for c in [
    "code_primary",
    "code_secondary",
    "evidence_quote_or_event",
    "mechanism_summary",
    "design_implication",
]:
    coding[c] = ""
coding.to_csv(OUT_CODING, index=False)


# ======================
# Write clickable HTML
# ======================
dfh = sample.copy()
dfh.insert(
    0,
    "open",
    dfh["html_url"].apply(
        lambda u: f'<a href="{u}" target="_blank">open</a>'
        if isinstance(u, str) and u.startswith("http") else ""
    ),
)

html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<style>
body {{ font-family: system-ui; margin: 24px; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
th {{ background: #fafafa; position: sticky; top: 0; }}
</style>
</head>
<body>
<h2>Qualitative Sample (n = {len(sample)})</h2>
<p>Click <b>open</b> to inspect PRs on GitHub.</p>
{dfh.to_html(index=False, escape=False)}
</body>
</html>
"""
Path(OUT_HTML).write_text(html, encoding="utf-8")


# ======================
# Sanity checks
# ======================
print("✓ Wrote:", OUT_CSV)
print("✓ Wrote:", OUT_CODING)
print("✓ Wrote:", OUT_HTML)
print("\nCounts by bucket and outcome:")
print(sample.groupby(["bucket", "y_merged"]).size())
