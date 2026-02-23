# pipeline/run_stats.py
import os, sys, duckdb, pandas as pd, yaml

HERE = os.path.dirname(os.path.abspath(__file__))

def connect_db(db_path): return duckdb.connect(database=db_path, read_only=False)

def create_views(con, data_dir):
    print(f"[bind] data_dir = {data_dir}")
    con.execute(f"""
      CREATE OR REPLACE VIEW pull_request        AS SELECT * FROM read_parquet('{data_dir}/pull_request.parquet');
      CREATE OR REPLACE VIEW pr_task_type        AS SELECT * FROM read_parquet('{data_dir}/pr_task_type.parquet');
      CREATE OR REPLACE VIEW pr_timeline         AS SELECT * FROM read_parquet('{data_dir}/pr_timeline.parquet');
      CREATE OR REPLACE VIEW pr_commits          AS SELECT * FROM read_parquet('{data_dir}/pr_commits.parquet');
      CREATE OR REPLACE VIEW pr_commit_details   AS SELECT * FROM read_parquet('{data_dir}/pr_commit_details.parquet');
      CREATE OR REPLACE VIEW pr_reviews          AS SELECT * FROM read_parquet('{data_dir}/pr_reviews.parquet');
      CREATE OR REPLACE VIEW repository          AS SELECT * FROM read_parquet('{data_dir}/repository.parquet');
      CREATE OR REPLACE VIEW "user"              AS SELECT * FROM read_parquet('{data_dir}/user.parquet');
    """)
    # quick existence sanity
    n_pr = con.execute("SELECT COUNT(*) FROM pull_request").fetchone()[0]
    n_tt = con.execute("SELECT COUNT(*) FROM pr_task_type").fetchone()[0]
    print(f"[bind] rows: pull_request={n_pr:,}  pr_task_type={n_tt:,}")

def main():
    # Anchor all paths to this folder; ignore cwd differences
    cfg_path = os.path.join(HERE, "config.yaml")
    if os.path.exists(cfg_path):
        cfg = yaml.safe_load(open(cfg_path))
        data_dir = os.path.join(HERE, cfg.get("data_dir", "data"))
        db_path  = os.path.join(HERE, cfg.get("db_path", "aid_dev.duckdb"))
    else:
        data_dir = os.path.join(HERE, "data")
        db_path  = os.path.join(HERE, "aid_dev.duckdb")

    out_dir = os.path.join(HERE, "outputs")
    os.makedirs(out_dir, exist_ok=True)

    print(f"[paths] HERE={HERE}")
    print(f"[paths] db_path={db_path}")
    print(f"[paths] out_dir={out_dir}")

    con = connect_db(db_path)
    create_views(con, data_dir)

    sql = r"""
    WITH base AS (
      SELECT pr.id AS pr_id, pr.user_id, pr.repo_id, tt.agent
      FROM pull_request pr
      JOIN pr_task_type tt ON tt.id = pr.id
      WHERE tt.agent IS NOT NULL
    ),
    by_agent AS (
      SELECT agent,
             COUNT(*)::BIGINT                AS pr_count,
             COUNT(DISTINCT user_id)::BIGINT AS developer_count,
             COUNT(DISTINCT repo_id)::BIGINT AS repo_count,
             0 AS sort_key
      FROM base GROUP BY agent
    ),
    totals AS (
      SELECT 'Total' AS agent,
             COUNT(*)::BIGINT,
             COUNT(DISTINCT user_id)::BIGINT,
             COUNT(DISTINCT repo_id)::BIGINT,
             1 AS sort_key
      FROM base
    )
    SELECT agent, pr_count, developer_count, repo_count
    FROM (SELECT * FROM by_agent UNION ALL SELECT * FROM totals) u
    ORDER BY sort_key, pr_count DESC;
    """

    print("[query] running stats…")
    df = con.execute(sql).df()
    print(f"[query] got {len(df)} rows")
    if len(df):
        print(df.to_string(index=False))

    csv_path = os.path.join(out_dir, "aid_overview_by_agent.csv")
    df.to_csv(csv_path, index=False)
    print(f"[write] wrote: {os.path.abspath(csv_path)}")
    print("[done]")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # surface any hidden errors so we know why the file didn't appear
        import traceback; traceback.print_exc(); sys.exit(1)
