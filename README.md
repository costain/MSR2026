# Replication Package  
**When AI Teammates Meet Code Review**  
MSR 2026 (Anonymous Submission)

This repository contains all code and analysis pipelines required to reproduce the results reported in the paper. The artifact supports:

- **RQ1**: Integration outcomes and decision latency  
- **RQ2**: Collaboration signals and logistic regression models  
- **Qualitative analysis** underlying Table 1  

All analyses are executed using Python and DuckDB over the AIDev dataset.  
No external SQL scripts are required; all queries are embedded directly in the Python code.

---

## 1. Environment Setup

### Requirements
- Python **3.10+**
- DuckDB **>= 0.10**
- pandas, numpy
- pyarrow
- statsmodels
- matplotlib

### Install dependencies
We recommend using a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 2. Dataset Setup (AIDev v3)

This artifact expects **AIDev v3** Parquet files to be placed in a directory named `AIDev/`.


### Required files
```
AIDev/
  pull_request.parquet
  pr_task_type.parquet
  pr_commits.parquet
  pr_commit_details.parquet
  pr_reviews.parquet
  pr_timeline.parquet
  repository.parquet
  user.parquet
```


> **Note:** The AIDev dataset is not redistributed with this artifact. Please download
> **AIDev v3** from its official Zenodo record  
> https://zenodo.org/records/16919272 (DOI: 10.5281/zenodo.16919272), released on
> November 5, 2025, and place the Parquet files as shown above.

---

## 3. Configuration

All scripts read configuration from `config.yaml`.

Example:
```yaml
data_dir: "AIDev"
db_path: "aid_dev.duckdb"
```

Update `data_dir` if your dataset is stored elsewhere.

---

## 4. RQ1: Integration Outcomes and Decision Latency

```bash
python run_rq1.py --config config.yaml
```

Outputs:
```
out/rq1/
```

---

## 5. RQ2: Feature Construction

```bash
python run_rq2.py --features out/rq2/rq2_features.csv --out_dir out/rq2
```

Output:
```
out/rq2/rq2_features.csv
```

---

## 6. RQ2: Logistic Regression Models

```bash
python run_rq2.py \
  --features out/rq2/rq2_features.csv \
  --out_dir out/rq2

```

Outputs:
```
out/rq2/
  rq2_logit_logcommits_cluster.csv
  rq2_logit_bucket_cluster.csv
  rq2_modelA_summary.txt
  rq2_modelB_summary.txt
```

---

## 7. Qualitative Analysis (Table 1)

Scripts:
- make_qual_sample.py
- make_qual_sample_from_features.py
- make_rq2_qual_ready.py

Codebook:
```
codebook_rq2_qualitative.md
```

Generated qualitative sample:
```
qual_sample.zip
```

---

## 8. Additional Statistics (Optional)

```bash
python run_stats.py --config config.yaml
```

---

## 9. Figures

Figures are generated automatically and saved under:
```
out/rq1/
out/rq2/
```

---

## 10. Reproducibility Notes

- Analyses are deterministic given the same input data.
- Random seeds are fixed.
- Results are associational, not causal.
- Post-merge signals (e.g., reverts) are intentionally excluded.

---

## 11. Artifact Scope

This artifact supports:
- Reproduction of all quantitative results
- Regeneration of all tables and figures
- Inspection of qualitative mechanisms

The AIDev dataset is not redistributed.

---

## 12. Contact

Anonymous for MSR 2026 review.
