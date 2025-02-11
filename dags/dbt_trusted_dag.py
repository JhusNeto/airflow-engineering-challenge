from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dbt_trusted_dag',
    default_args=default_args,
    description='Executa o DBT para transformar dados da Stage para Trusted',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'trusted']
) as dag:

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='cd /opt/airflow/dbt && dbt run'
    )

    validate_transformation = BashOperator(
        task_id='validate_transformation',
        bash_command='cd /opt/airflow/dbt && dbt test'
    )

    generate_documentation = BashOperator(
        task_id='generate_documentation',
        bash_command='cd /opt/airflow/dbt && dbt docs generate'
    )

    # Define o fluxo de execução das tarefas
    transform_data >> validate_transformation >> generate_documentation