
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

# --- Airflow Connection IDs ---
WRITEDB_CONN_ID = "writedb_conn"

@dag(
    dag_id="etl_final_processing_phase_2",
    start_date=datetime(2025, 7, 9, tzinfo=ZoneInfo("America/Toronto")),
    schedule='40 23/4 * * *',  # Same schedule as DAG 1 and DAG 2
    catchup=False,
    tags=["etl", "phase_2", "localstack", "final_processing"],
    doc_md="""
    ### ETL Final Processing - Phase 2

    This DAG represents the final processing phase, reading data from the 'writedb'
    (processed_data.pokemon_data) and performing further actions.

    It waits for the 'etl_from_readdb_to_writedb_phase_1' to complete successfully.

    **Connections Required:**
    - `writedb_conn`: Airflow connection for the writeable database (to read from).
    """,
)
def etl_final_processing_phase_2():
    # Sensor to wait for DAG 2 (etl_from_readdb_to_writedb_phase_1) to complete
    wait_for_phase_1_dag = ExternalTaskSensor(
        task_id="wait_for_etl_phase_1_dag",
        external_dag_id="etl_from_readdb_to_writedb_phase_1",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        poke_interval=10,
        timeout=3600,
    )

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
            # Add your actual final processing logic here (e.g., load to DWH, generate reports)
        except Exception as e:
            logging.error(f"Error during final processing from writedb: {e}")
            raise

    # Define the source details for final processing.
    SOURCE_TABLE_PHASE_2 = "pokemon_data"
    SOURCE_SCHEMA_PHASE_2 = "processed_data"

    final_processing_task = process_data_from_writedb(
        conn_id=WRITEDB_CONN_ID,
        source_table=SOURCE_TABLE_PHASE_2,
        source_schema=SOURCE_SCHEMA_PHASE_2
    )

    wait_for_phase_1_dag >> final_processing_task

etl_final_processing_phase_2()
