from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

default_args = {
    'owner': 'retail_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'retail_analytics_pipeline',
    default_args=default_args,
    description='End-to-end retail data pipeline with two fact tables',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['retail', 'pyspark']
) as dag:

    generate_data = BashOperator(
        task_id='generate_sample_data',
        bash_command='python /opt/airflow/scripts/generate_data.py',
        dag=dag
    )

    run_etl = BashOperator(
        task_id='run_pyspark_etl',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/airflow/scripts/etl/process_data.py',
        dag=dag
    )

    create_views = MySqlOperator(
        task_id='create_analytics_views',
        mysql_conn_id='mysql_retail',
        sql='sql/schema/analytics_views.sql',
        dag=dag
    )

    generate_reports = BashOperator(
        task_id='generate_reports',
        bash_command='python /opt/airflow/scripts/analytics/generate_reports.py',
        dag=dag
    )

    generate_data >> run_etl >> create_views >> generate_reports
