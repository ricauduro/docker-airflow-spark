from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from datetime import datetime

dt = datetime.strftime(datetime.now(),"%Y%m%d%H%M%S")

def push_xcom_data(**kwargs):
    kwargs["ti"].xcom_push(key="date", value=dt)

# Define default arguments
default_args = {
    "owner": "rica7",
    "depends_on_past": False,
    "start_date": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    "email": ["ricardo.cauduro@outlook.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1)
}

# Define the DAG
with DAG(
    dag_id="upload_from_api",
    default_args=default_args,
    description="Run pipeline with docker operator in Airflow locally",
    schedule_interval=None,
) as dag:
    
    # Define the dataset creation task
    push_xcom_task = PythonOperator(
        task_id="push_xcom_task",
        python_callable=push_xcom_data,
        provide_context=True,
        dag=dag
    )

    file_to_table = BashOperator(
        task_id="execute_notebook",
        bash_command="docker exec airflow-jupyter-1 sh -c 'pip install pyarrow && pip install findspark && pip install hdfs3 && pip install papermill && papermill /home/jovyan/work/teste.ipynb /home/jovyan/work/out_teste.ipynb -p date \"{{ ti.xcom_pull(task_ids=\'push_xcom_task\', key=\'date\') }}\"'",
        dag=dag
    )


    # Define the task dependencies
    push_xcom_task >> file_to_table
