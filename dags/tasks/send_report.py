import os
from airflow.operators.email_operator import EmailOperator
def send_report():
    return EmailOperator(
        task_id='send_email_task',
        to='shahedhesham13@gmail.com',
        subject='Daily Metrics Report',
        html_content="Find valuable business insights with the daily Metrics Report attached",
        files=['/opt/airflow/data/metrics_report.txt']    
)