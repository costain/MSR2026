#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf


def fit_logit_cluster(df: pd.DataFrame, formula: str, cluster_col: str):
    model = smf.logit(formula=formula, data=df)
    # Fit with cluster-robust covariance directly (compatible across statsmodels versions)
    res = model.fit(
        disp=False,
        cov_type="cluster",
        cov_kwds={"groups": df[cluster_col]},
    )
    return res



def tidy_or_table(res):
    params = res.params
    se = res.bse
    pvals = res.pvalues

    # Odds ratios
    or_ = np.exp(params)
    ci_low = np.exp(params - 1.96 * se)
    ci_high = np.exp(params + 1.96 * se)

    out = pd.DataFrame({
        "term": params.index,
        "coef": params.values,
        "OR": or_.values,
        "CI_low": ci_low.values,
        "CI_high": ci_high.values,
        "p": pvals.values
    })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", default="out/rq2/rq2_features.csv")
    ap.add_argument("--out_dir", default="out/rq2")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.features)

    # Keep only valid rows
    df = df.dropna(subset=["y_merged", "repo_id", "agent"]).copy()
    df["repo_id"] = df["repo_id"].astype(str)
    df["agent"] = df["agent"].astype(str)

    # Model A: commit buckets
    fA = (
        "y_merged ~ C(commit_bucket) + log1p_delta_loc + log1p_files + "
        "tests_added + force_push + has_review + log1p_hours_to_first_review + C(agent)"
    )

    # Model B: raw commits log1p
    fB = (
        "y_merged ~ log1p_commits + log1p_delta_loc + log1p_files + "
        "tests_added + force_push + has_review + log1p_hours_to_first_review + C(agent)"
    )

    resA = fit_logit_cluster(df, fA, "repo_id")
    resB = fit_logit_cluster(df, fB, "repo_id")

    tabA = tidy_or_table(resA)
    tabB = tidy_or_table(resB)

    tabA.to_csv(out_dir / "rq2_logit_bucket_cluster.csv", index=False)
    tabB.to_csv(out_dir / "rq2_logit_logcommits_cluster.csv", index=False)

    # Quick text summaries
    with open(out_dir / "rq2_modelA_summary.txt", "w") as f:
        f.write(resA.summary().as_text())
    with open(out_dir / "rq2_modelB_summary.txt", "w") as f:
        f.write(resB.summary().as_text())

    print("✓ RQ2 done:")
    print(f"  - {out_dir/'rq2_logit_bucket_cluster.csv'}")
    print(f"  - {out_dir/'rq2_logit_logcommits_cluster.csv'}")
    print(f"  - {out_dir/'rq2_modelA_summary.txt'}")
    print(f"  - {out_dir/'rq2_modelB_summary.txt'}")


if __name__ == "__main__":
    main()
