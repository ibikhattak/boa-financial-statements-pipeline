import pandas as pd
from datetime import datetime
from sqlalchemy import text
from src.dq_rules import run_all_dq_checks


# ---------------------------------------------------------
# Insert ETL Log Start
# ---------------------------------------------------------
def insert_etl_log_start(engine, file_name):
    sql = """
        INSERT INTO etl_log (file_name, started_at, status)
        OUTPUT INSERTED.run_id
        VALUES (:file_name, :started_at, 'RUNNING');
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {"file_name": file_name, "started_at": datetime.utcnow()}
        )
        return result.fetchone().run_id


# ---------------------------------------------------------
# Insert ETL Log End
# ---------------------------------------------------------
def insert_etl_log_end(engine, run_id, rows_loaded, rows_failed, status, message=None):
    sql = """
        UPDATE etl_log
        SET completed_at = :completed_at,
            rows_loaded = :rows_loaded,
            rows_failed = :rows_failed,
            status = :status,
            message = :message
        WHERE run_id = :run_id;
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "completed_at": datetime.utcnow(),
                "rows_loaded": rows_loaded,
                "rows_failed": rows_failed,
                "status": status,
                "message": message,
                "run_id": run_id
            }
        )


# ---------------------------------------------------------
# Insert DQ Issues
# ---------------------------------------------------------
def insert_dq_issues(engine, issues, run_id):
    if not issues:
        return

    df = pd.DataFrame(issues)
    df["run_id"] = run_id
    df.drop(columns=["row_index"], inplace=True)

    df.to_sql(
        "dq_issues",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=500
    )


# ---------------------------------------------------------
# Insert ALL Data (even with DQ issues)
# ---------------------------------------------------------
def insert_all_data(engine, df):
    df.to_sql(
        "provider_specific_file",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=500
    )


# ---------------------------------------------------------
# Main ETL Runner
# ---------------------------------------------------------
def run_etl(csv_path, engine):
    file_name = csv_path.split("/")[-1]
    run_id = insert_etl_log_start(engine, file_name)

    try:
        # Load CSV
        df = pd.read_csv(csv_path, dtype=str)

        # Run DQ checks
        issues = run_all_dq_checks(df)

        # Insert ALL rows into provider_specific_file
        insert_all_data(engine, df)

        # Insert DQ issues for later analysis
        insert_dq_issues(engine, issues, run_id)

        # Log ETL summary
        insert_etl_log_end(
            engine,
            run_id,
            rows_loaded=len(df),
            rows_failed=len(issues),
            status="SUCCESS"
        )

        print("ETL completed successfully")

    except Exception as e:
        insert_etl_log_end(
            engine,
            run_id,
            rows_loaded=0,
            rows_failed=0,
            status="FAILED",
            message=str(e)
        )
        raise