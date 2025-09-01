import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Airflow Connection IDs ---
READDB_CONN_ID = "readdb_conn"
WRITEDB_CONN_ID = "writedb_conn"


@dag(
    dag_id="etl_from_readdb_to_writedb_phase_1",
    start_date=datetime(2025, 7, 9, tzinfo=ZoneInfo("America/Toronto")),
    schedule=None,  # This DAG is now triggered by pokemon_to_s3_to_db_dag
    catchup=False,
    tags=["etl", "phase_1", "localstack", "data_transfer"],
    doc_md="""
    ### ETL from ReadDB to WriteDB - Phase 1

    This DAG reads data from the 'readdb' (source_tables.pokemon) and writes it
    to the 'writedb' (processed_data.pokemon_data).

    It waits for the 'pokemon_to_s3_to_db_dag' to complete successfully before starting.

    **Connections Required:**
    - `readdb_conn`: Airflow connection for the read-only database.
    - `writedb_conn`: Airflow connection for the writeable database.
    """,
)
def etl_from_readdb_to_writedb_phase_1():

    @task
    def transfer_data_from_readdb_to_writedb(
        read_conn_id: str, write_conn_id: str, source_table: str, target_table: str, target_schema: str
    ):
        """
        Reads data from a specified source_table in readdb and writes it to
        a specified target_table in writedb.
        """
        logging.info(f"Starting data transfer from {source_table} (readdb) to {target_schema}.{target_table} (writedb)")

        # Initialize hooks
        read_hook = PostgresHook(postgres_conn_id=read_conn_id)
        write_hook = PostgresHook(postgres_conn_id=write_conn_id)

        # --- Read data from readdb ---
        try:
            read_conn = read_hook.get_conn()
            read_cursor = read_conn.cursor()
            read_cursor.execute(f"SELECT id, name, url FROM {source_table};")
            data_to_transfer = read_cursor.fetchall()
            read_cursor.close()
            read_conn.close()
            logging.info(f"Successfully read {len(data_to_transfer)} records from {source_table}.")
        except Exception as e:
            logging.error(f"Error reading from readdb: {e}")
            raise

        if not data_to_transfer:
            logging.info("No data to transfer. Exiting.")
            return

        # --- Write data to writedb ---
        try:
            write_conn = write_hook.get_conn()
            write_cursor = write_conn.cursor()

            # Create schema if it doesn't exist
            write_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")

            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                url VARCHAR(255)
            );
            """
            write_cursor.execute(create_table_sql)

            # Insert data
            insert_sql = f"""
            INSERT INTO {target_schema}.{target_table} (id, name, url)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                url = EXCLUDED.url;
            """
            write_cursor.executemany(insert_sql, data_to_transfer)
            write_conn.commit()
            write_cursor.close()
            write_conn.close()
            logging.info(f"Successfully wrote {len(data_to_transfer)} records to {target_schema}.{target_table}.")
        except Exception as e:
            logging.error(f"Error writing to writedb: {e}")
            raise

    # Define the source and target details.
    SOURCE_TABLE = "source_tables.pokemon"
    TARGET_TABLE = "pokemon_data"
    TARGET_SCHEMA = "processed_data"

    transfer_task = transfer_data_from_readdb_to_writedb(
        read_conn_id=READDB_CONN_ID,
        write_conn_id=WRITEDB_CONN_ID,
        source_table=SOURCE_TABLE,
        target_table=TARGET_TABLE,
        target_schema=TARGET_SCHEMA,
    )

    trigger_phase_2_dag = TriggerDagRunOperator(
        task_id="trigger_etl_phase_2_dag",
        trigger_dag_id="etl_final_processing_phase_2",
        wait_for_completion=False,
    )

    transfer_task >> trigger_phase_2_dag


etl_from_readdb_to_writedb_phase_1()