# When AI Teammates Meet Code Review  
## Collaboration Signals Shaping the Integration of Agent-Authored Pull Requests  

[![Conference](https://img.shields.io/badge/Conference-MSR%202026-blue)]
[![Status](https://img.shields.io/badge/Status-Accepted-yellow)]

📄 **Accepted at:**  
23rd International Conference on Mining Software Repositories (MSR 2026)  
Rio de Janeiro, Brazil  

**Status:** Accepted, to appear.

**Authors:**  
Costain Nachuma (Idaho State University)  
Minhaz F. Zibran (Idaho State University)

---

## 🧠 Research Context

Autonomous coding agents are increasingly submitting pull requests on GitHub.  
This study investigates how agent-authored pull requests integrate into human-driven review workflows.

Rather than treating agent contributions purely as code artifacts, we analyze their integration as a **socio-technical process**, examining:

- Integration outcomes (merge vs. non-merge)
- Decision latency
- Review-time collaboration signals
- Coordination stability
- Reviewer engagement dynamics

Paper reference: :contentReference[oaicite:1]{index=1}

---

## 🔬 Research Questions

### RQ1
What proportions of agent-authored PRs are merged, closed without merging, or remain open — and how long do they take to reach a decision?

### RQ2
Which review-time collaboration signals are associated with successful integration?

Signals include:
- Iteration intensity
- Change magnitude (ΔLOC, files)
- Force pushes
- Reviewer presence
- Time to first review
- Testing behavior

---

## 🏗 Methodology Overview

- Dataset: **AIDev v3**
- 33,596 agent-authored PRs
- 2,807 repositories
- Logistic regression with repository-clustered standard errors
- Qualitative coding of 60 PRs
- DuckDB-based analytical pipeline

All models are associational, not causal.

---

## ⚙️ Reproducibility Instructions

### Requirements

- Python 3.10+
- DuckDB >= 0.10
- pandas
- numpy
- pyarrow
- statsmodels
- matplotlib

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
