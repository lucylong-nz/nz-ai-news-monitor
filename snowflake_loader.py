"""
snowflake_loader.py

Snowflake Loader Class
Project: AI News Monitor
"""

from __future__ import annotations

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import date, datetime, timezone
from typing import Optional

from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_RAW_SCHEMA,
    DBT_SCHEMA,
)


class SnowflakeLoader:

    def __init__(self):

        self.conn = None

    # ---------------------------------------------------
    # Connect
    # ---------------------------------------------------

    def connect(self):

        self.conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_RAW_SCHEMA,
        )

        print("Connected to Snowflake.")

    # ---------------------------------------------------
    # Close
    # ---------------------------------------------------

    def close(self):

        if self.conn:

            self.conn.close()

            print("Snowflake connection closed.")

    # ---------------------------------------------------
    # Upload DataFrame to Landing
    # Upload the fetched DataFrame into the LANDING table. It overwrites the previous landing batch.
    # ---------------------------------------------------

    def upload_landing(self, df, table_name):

        print(f"Uploading {len(df)} rows to LANDING...")

        success, nchunks, nrows, _ = write_pandas(
            conn=self.conn,
            df=df,
            table_name=table_name,
            auto_create_table=False,
            overwrite=True,
            use_logical_type=True,
        )

        if not success:
            raise Exception("Landing upload failed.")

        print(f"{nrows} rows uploaded to LANDING.")

        return nrows

    # ---------------------------------------------------
    # Merge
    # Merge the landing table into the RAW table. It performs an upsert based on the primary key.
    # ---------------------------------------------------

    def execute_sql_file(self, filename):

        with open(filename, "r") as f:
            sql = f.read()

        cur = self.conn.cursor()

        cur.execute(sql)

        cur.close()

    # ---------------------------------------------------
    # Dynamic Pipeline Checkpoint
    # Find the latest successful FETCH_TO from the pipeline log. This provides the checkpoint for the next incremental run.
    # ---------------------------------------------------

    def get_last_successful_fetch_to(self) -> Optional[date]:
        """
        Return the latest successful FETCH_TO checkpoint.

        DAILY_INCREMENTAL is preferred. If no successful daily run exists,
        fall back to the most recent successful INITIAL_LOAD.
        """

        sql = """
            SELECT FETCH_TO
            FROM RAW.PIPELINE_RUN_LOG
            WHERE STATUS = 'SUCCESS'
            AND FETCH_TO IS NOT NULL
            AND PIPELINE_NAME IN (
                'DAILY_INCREMENTAL',
                'INITIAL_LOAD'
            )
            ORDER BY
                CASE
                    WHEN PIPELINE_NAME = 'DAILY_INCREMENTAL' THEN 1
                    ELSE 2
                END,
                END_TIME DESC
            LIMIT 1
        """

        cursor = self.conn.cursor()

        try:
            cursor.execute(sql)
            row = cursor.fetchone()

            if row is None:
                return None

            return row[0]

        finally:
            cursor.close()

    # ---------------------------------------------------
    # Pipeline Log
    # ---------------------------------------------------

    def write_log(
        self,
        run_id,
        pipeline_name,
        status,
        rows_inserted,
        comments=""
    ):

        sql = """

        INSERT INTO RAW.PIPELINE_RUN_LOG
        (
            RUN_ID,
            PIPELINE_NAME,
            START_TIME,
            END_TIME,
            STATUS,
            ROWS_INSERTED,
            COMMENTS
        )

        VALUES
        (
            %s,%s,%s,%s,%s,%s,%s
        )

        """

        now = datetime.utcnow()

        cur = self.conn.cursor()

        cur.execute(

            sql,

            (
                run_id,
                pipeline_name,
                now,
                now,
                status,
                rows_inserted,
                comments,
            ),
        )

        cur.close()

        print("Pipeline log written.")
        
    # ---------------------------------------------------
    # Start Pipeline Run
    # Create a log record with RUNNING, FETCH_FROM, FETCH_TO, start time, etc.
    # ---------------------------------------------------

        
    def start_pipeline_run(
        self,
        run_id: str,
        pipeline_name: str,
        fetch_from: date,
        fetch_to: date,
    ) -> None:
        """
        Insert a RUNNING pipeline record.
        """

        sql = """
            INSERT INTO RAW.PIPELINE_RUN_LOG
            (
                RUN_ID,
                PIPELINE_NAME,
                START_TIME,
                END_TIME,
                STATUS,
                ROWS_INSERTED,
                COMMENTS,
                FETCH_FROM,
                FETCH_TO,
                ROWS_FETCHED,
                ROWS_STAGED
            )
            VALUES
            (
                %s,
                %s,
                %s,
                NULL,
                'RUNNING',
                0,
                '',
                %s,
                %s,
                0,
                0
            )
        """

        started_at = datetime.now(timezone.utc).replace(tzinfo=None)

        cursor = self.conn.cursor()

        try:
            cursor.execute(
                sql,
                (
                    run_id,
                    pipeline_name,
                    started_at,
                    fetch_from,
                    fetch_to,
                ),
            )

        finally:
            cursor.close()

        print(
            f"Pipeline run started: {run_id} "
            f"({fetch_from} to {fetch_to})"
        )
        
    # ---------------------------------------------------
    # Complete Pipeline Run
    # Change the run to SUCCESS and record fetched/staged/inserted row counts.
    # ---------------------------------------------------
        
        
    def complete_pipeline_run(
        self,
        run_id: str,
        rows_fetched: int,
        rows_staged: int,
        rows_inserted: int,
        comments: str = "",
    ) -> None:
        """
        Mark an existing pipeline run as successful.
        """

        sql = """
            UPDATE RAW.PIPELINE_RUN_LOG
            SET
                END_TIME = %s,
                STATUS = 'SUCCESS',
                ROWS_FETCHED = %s,
                ROWS_STAGED = %s,
                ROWS_INSERTED = %s,
                COMMENTS = %s
            WHERE RUN_ID = %s
        """

        ended_at = datetime.now(timezone.utc).replace(tzinfo=None)

        cursor = self.conn.cursor()

        try:
            cursor.execute(
                sql,
                (
                    ended_at,
                    rows_fetched,
                    rows_staged,
                    rows_inserted,
                    comments,
                    run_id,
                ),
            )

        finally:
            cursor.close()

        print(f"Pipeline run completed successfully: {run_id}")
        
    # ---------------------------------------------------
    # Fail Pipeline Run
    # Change the run to FAILED and save the error message.
    # ---------------------------------------------------
    def fail_pipeline_run(
        self,
        run_id: str,
        comments: str,
    ) -> None:
        """
        Mark an existing pipeline run as failed.
        """

        sql = """
            UPDATE RAW.PIPELINE_RUN_LOG
            SET
                END_TIME = %s,
                STATUS = 'FAILED',
                COMMENTS = %s
            WHERE RUN_ID = %s
        """

        ended_at = datetime.now(timezone.utc).replace(tzinfo=None)

        cursor = self.conn.cursor()

        try:
            cursor.execute(
                sql,
                (
                    ended_at,
                    comments[:1000],
                    run_id,
                ),
            )

        finally:
            cursor.close()

        print(f"Pipeline run failed: {run_id}")   
        
        
    def get_table_row_count(
        self,
        table_name: str,
        schema_name: str = "RAW",
    ) -> int:
        """
        Return the total row count for a Snowflake table.
        """

        sql = f"""
            SELECT COUNT(*)
            FROM {schema_name}.{table_name}
        """

        cursor = self.conn.cursor()

        try:
            cursor.execute(sql)
            row = cursor.fetchone()

            return int(row[0])

        finally:
            cursor.close()