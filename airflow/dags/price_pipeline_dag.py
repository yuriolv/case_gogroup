from datetime import timedelta, datetime
import logging

from airflow.sdk import DAG, TaskGroup, Variable, Connection
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from ingestion.producers.producer import KabumSmartphonesProducer
from ingestion.consumers.consumer import KabumConsumer

logger = logging.getLogger(__name__)


def extract_and_publish():
    logger.info("Iniciando producer de smartphones")

    producer = KabumSmartphonesProducer()
    producer.run()

    logger.info("Producer finalizado")


def consume_and_store():
    logger.info("Iniciando consumer Kafka")

    postgres_conn = Connection.get("POSTGRES_DEFAULT")

    conn_str = (
        f"host={postgres_conn.host} "
        f"port={postgres_conn.port} "
        f"dbname={postgres_conn.schema} "
        f"user={postgres_conn.login} "
        f"password={postgres_conn.password}"
    )

    consumer = KabumConsumer(postgres_conn=conn_str)
    consumer.run()

    logger.info("Consumer finalizado")


def get_dbt_path():
    return Variable.get("DBT_PATH")


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="price_pipeline_hourly",
    description="Pipeline de preços com Kafka + Postgres + dbt",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["case", "pricing"],
) as dag:

    with TaskGroup(group_id="ingestion") as ingestion:

        extract = PythonOperator(
            task_id="extract_and_publish",
            python_callable=extract_and_publish,
        )

        load = PythonOperator(
            task_id="consume_and_store",
            python_callable=consume_and_store,
        )

        extract >> load

    with TaskGroup(group_id="transformation") as transformation:

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"""
            cd {get_dbt_path()} && dbt run --profiles-dir .
            """,
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"""
            cd {get_dbt_path()} && dbt test --profiles-dir .
            """,
        )

        dbt_docs = BashOperator(
            task_id="dbt_docs",
            bash_command=f"""
            cd {get_dbt_path()} && dbt docs generate --profiles-dir .
            """,
        )

        dbt_run >> dbt_test >> dbt_docs

    ingestion >> transformation