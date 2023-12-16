from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import sys

code_path = Variable.get("scripts_path")
sys.path.insert(0, code_path)

from extract_data import extract_data_main
from merge_data import merge_data_main

default_args = {
    "owner": "amith.nandivada",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    'timezone': 'Asia/Kolkata'
}

dag = DAG(
    dag_id="HousePricePredictionOrchestrationDAG",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

start = BashOperator(task_id="START", bash_command="echo start", dag=dag)

extract_data_from_api =  PythonOperator(task_id = "EXTRACT_DATA_FROM_API",
                                            python_callable = extract_data_main,
                                            dag = dag)

merge_data_from_landing_zone = PythonOperator(task_id = "MERGE_DATA_FROM_LANDING_ZONE",
                                            python_callable = merge_data_main,
                                            dag = dag)

end = BashOperator(task_id="END", bash_command="echo end", dag=dag)

start >> extract_data_from_api >> merge_data_from_landing_zone >> end