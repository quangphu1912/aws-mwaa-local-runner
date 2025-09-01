import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

# --- Airflow Connection IDs ---
AWS_CONN_ID = "aws_default"
DB_CONN_ID = "readdb_conn"  # Default to readdb, can be changed via connections


@dag(
    dag_id="pokemon_to_s3_to_db_dag",
    start_date=datetime(2025, 7, 9, tzinfo=ZoneInfo("America/Toronto")),
    schedule='15 0 * * *',
    catchup=False,
    tags=["example", "localstack", "pokemon"],
    # Define default parameters for the DAG. These can be overridden in the UI.
    params={
        "s3_bucket": "my-local-pokemon-bucket",
        "s3_key": "pokemon/data.json",
        "db_table": "pokemon",
        "db_schema": "source_tables",
        "db_conn_id": "readdb_conn",  # NEW: Parameter for the connection ID
    },
    doc_md="""
    ### Pokémon API to S3 to DB DAG (Hook & Schema Version)

    This DAG demonstrates a best-practice workflow using Airflow Hooks and Schemas:
    1.  **Fetch Data**: Fetches Pokémon data from the PokéAPI.
    2.  **Upload to S3**: Uses `S3Hook` to connect to S3 (LocalStack) and upload the data.
    3.  **Load to Database**: Uses `PostgresHook` to write the data to a specific schema and table.

    **Configuration:**
    - `s3_bucket` (Param): The S3 bucket to use.
    - `s3_key` (Param): The S3 key (path) for the data file.
    - `db_table` (Param): The database table to write to.
    - `db_schema` (Param): The database schema to use. Default: `source_tables`.
    """,
)
def pokemon_to_s3_to_db_dag():
    """
    DAG to fetch Pokémon data, store it in S3, and then write to a database.
    This version uses Airflow Hooks for all external connections and supports schemas.
    """

    @task
    def fetch_pokemon_data():
        """Fetches a list of Pokémon from the PokéAPI."""
        logging.info("Fetching data from PokéAPI...")
        response = requests.get("https://pokeapi.co/api/v2/pokemon?limit=151")  # Gen 1
        response.raise_for_status()
        logging.info("Data fetched successfully.")
        return response.json()['results']

    @task
    def upload_to_s3(data: list, s3_bucket: str, s3_key: str):
        """Uses S3Hook to upload a list of dictionaries to an S3 bucket."""
        logging.info(f"Uploading data to s3://{s3_bucket}/{s3_key}")
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_client = s3_hook.get_conn()

        try:
            s3_client.create_bucket(Bucket=s3_bucket)
            logging.info(f"Bucket '{s3_bucket}' created or already exists.")
        except Exception as e:
            logging.error(f"Error creating bucket: {e}")
            raise

        s3_hook.load_string(
            string_data=json.dumps(data, indent=4),
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True,
        )
        logging.info("Data uploaded successfully using S3Hook.")
        return s3_key

    @task
    def s3_to_database(s3_key: str, s3_bucket: str, table_name: str, schema_name: str, db_conn_id: str):
        """Downloads data from S3 and writes it to a specific schema in a PostgreSQL database."""
        logging.info(f"Writing data from s3://{s3_bucket}/{s3_key} to table '{schema_name}.{table_name}'...")

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        file_content = s3_hook.read_key(key=s3_key, bucket_name=s3_bucket)
        data = json.loads(file_content)

        pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create the schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            url VARCHAR(255)
        );
        """
        cursor.execute(create_table_sql)

        for pokemon in data:
            insert_sql = f"""
            INSERT INTO {schema_name}.{table_name} (name, url)
            VALUES (%s, %s)
            ON CONFLICT (name) DO NOTHING;
            """
            cursor.execute(insert_sql, (pokemon["name"], pokemon["url"]))

        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Successfully inserted/updated records into '{schema_name}.{table_name}'.")

    # --- Define the task dependencies ---
    S3_BUCKET = "{{ params.s3_bucket }}"
    S3_KEY_PATH = "{{ params.s3_key }}"
    DB_TABLE = "{{ params.db_table }}"
    DB_SCHEMA = "{{ params.db_schema }}"
    DB_CONN_ID_PARAM = "{{ params.db_conn_id }}"  # NEW: Get conn_id from params

    api_data = fetch_pokemon_data()
    s3_path = upload_to_s3(api_data, s3_bucket=S3_BUCKET, s3_key=S3_KEY_PATH)
    s3_to_db_task = s3_to_database(
        s3_path, s3_bucket=S3_BUCKET, table_name=DB_TABLE, schema_name=DB_SCHEMA, db_conn_id=DB_CONN_ID_PARAM
    )

    # NEW: Add a task to trigger the next DAG in the pipeline
    trigger_phase_1_dag = TriggerDagRunOperator(
        task_id="trigger_etl_phase_1_dag",
        trigger_dag_id="etl_from_readdb_to_writedb_phase_1",
        wait_for_completion=False,  # Set to True if you want this DAG to wait for the triggered DAG
    )

    s3_to_db_task >> trigger_phase_1_dag


# Instantiate the DAG
pokemon_to_s3_to_db_dag()
