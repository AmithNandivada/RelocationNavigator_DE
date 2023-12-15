from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import sys

code_path = Variable.get("scripts_path")
sys.path.insert(0, code_path)

from Extract_2 import main


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

# run_test_script = BashOperator(
#     task_id='run_test_script',
#     bash_command='cd /opt/airflow/scripts; ls; pwd; python hello.py',
#     dag = dag
# )

# extract_data_from_api = BashOperator(
#     task_id='EXTRACT_DATA_FROM_API',
#     bash_command='cd /opt/airflow/scripts; python Extract_2.py',
#     dag = dag
# )

extract_data_from_api =  PythonOperator(task_id = "EXTRACT_DATA_FROM_API",
                                            python_callable = main,
                                            dag = dag)

end = BashOperator(task_id="END", bash_command="echo end", dag=dag)

start >> extract_data_from_api >> end