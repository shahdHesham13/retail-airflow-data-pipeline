from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from tasks.create_tables import create_tables
from tasks.generate_data import (
    generate_customers,
    generate_products,
    generate_stores,
    generate_transactions
)
from tasks.calculate_metrics import calculate_metrics
from tasks.send_report import send_report
from tasks.cleanup import delete_files

import sys
print(sys.executable)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    create_data_dir_task = BashOperator(
        task_id='create_data_dir',
        bash_command='mkdir -p /opt/airflow/data'
    )

    create_tables_task = PythonOperator(
        task_id='create_tables_task',
        python_callable=create_tables
    )

    generate_customers_task = PythonOperator(
        task_id='generate_customers_task',
        python_callable=generate_customers
    )

    generate_products_task = PythonOperator(
        task_id='generate_products_task',
        python_callable=generate_products
    )

    generate_stores_task = PythonOperator(
        task_id='generate_stores_task',
        python_callable=generate_stores
    )

    generate_transactions_task = PythonOperator(
        task_id='generate_transactions_task',
        python_callable=generate_transactions
    )

    calculate_metrics_task = PythonOperator(
        task_id='calculate_metrics_task',
        python_callable=calculate_metrics
    )

    send_email_task = send_report()
#     send_email_task = PythonOperator(
#     task_id='send_email_task',
#     python_callable=lambda: print("Skipping email task.")
# )


    delete_files_task = PythonOperator(
        task_id='delete_files_task',
        python_callable=delete_files
    )

    # Define the task order
    create_data_dir_task >> create_tables_task
    create_tables_task >> generate_customers_task
    create_tables_task >> generate_products_task
    create_tables_task >> generate_stores_task
    create_tables_task >> generate_transactions_task
    [generate_customers_task, generate_products_task, generate_stores_task] >> generate_transactions_task
    generate_transactions_task >> calculate_metrics_task
    calculate_metrics_task >> send_email_task
    send_email_task >> delete_files_task