import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Airflow Connection IDs ---
WRITEDB_CONN_ID = "writedb_conn"

@dag(
    dag_id="etl_final_processing_phase_2",
    start_date=datetime(2025, 7, 9, tzinfo=ZoneInfo("America/Toronto")),
    schedule=None,  # This DAG is now triggered by etl_from_readdb_to_writedb_phase_1
    catchup=False,
    tags=["etl", "phase_2", "localstack", "final_processing"],
    doc_md="""
    ### ETL Final Processing - Phase 2

    This DAG represents the final processing phase.
    
    **This DAG is triggered by `etl_from_readdb_to_writedb_phase_1`.**
    """,
)
def etl_final_processing_phase_2():

    @task
    def process_data_from_writedb(conn_id: str, source_table: str, source_schema: str):
        """
        Reads data from the writedb and performs final processing.
        For this example, it just logs the count of records.
        """
        logging.info(f"Starting final processing from {source_schema}.{source_table} (writedb)")
        read_hook = PostgresHook(postgres_conn_id=conn_id)
        try:
            read_conn = read_hook.get_conn()
            read_cursor = read_conn.cursor()
            read_cursor.execute(f"SELECT COUNT(*) FROM {source_schema}.{source_table};")
            record_count = read_cursor.fetchone()[0]
            read_cursor.close()
            read_conn.close()
            logging.info(f"Successfully read {record_count} records from {source_schema}.{source_table} for final processing.")
        except Exception as e:
            logging.error(f"Error during final processing from writedb: {e}")
            raise

    SOURCE_TABLE_PHASE_2 = "pokemon_data"
    SOURCE_SCHEMA_PHASE_2 = "processed_data"

    process_data_from_writedb(
        conn_id=WRITEDB_CONN_ID,
        source_table=SOURCE_TABLE_PHASE_2,
        source_schema=SOURCE_SCHEMA_PHASE_2
    )

etl_final_processing_phase_2()