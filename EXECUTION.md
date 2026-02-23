## 🚀 Full Reproduction (Copy & Run)

```bash
# Create virtual environment
python -m venv .venv

# Activate environment (macOS/Linux)
source .venv/bin/activate

# If using Windows PowerShell, use instead:
# .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run RQ1
python run_rq1.py --config config.yaml

# Build RQ2 features
python build_rq2_features.py --config config.yaml

# Run RQ2 logistic regression models
python run_rq2.py --features out/rq2/rq2_features.csv --out_dir out/rq2

# Generate qualitative sample (optional)
python make_qual_sample.py --config config.yaml

# Alternative qualitative sampling (optional)
python make_qual_sample_from_features.py --config config.yaml

# Prepare qualitative coding sheet (optional)
python make_rq2_qual_ready.py --config config.yaml

# Run additional statistical checks (optional)
python run_stats.py --config config.yaml
```
