import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Add json key for service account to scripts path and add it to connections. 
# connection id: google_cloud_default, Connection Type: Google Cloud, Keyfile Path: /opt/airflow/scripts/json_key.json
# Also add /opt/airflow/scripts/ path to variables with name scripts_path

code_path = Variable.get("scripts_path")
sys.path.insert(0, code_path)

from extract_data import extract_data_main
from merge_data import merge_data_main
from clean_data import clean_data_main
from transform_data import transform_data_main

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

clean_data = PythonOperator(task_id = "CLEAN_DATA",
                            python_callable = clean_data_main,
                            dag = dag)

transform_data = PythonOperator(task_id = "TRANSFORM_DATA",
                            python_callable = transform_data_main,
                            dag = dag)

create_property_table = GCSToBigQueryOperator(
    task_id=f"CREATE_PROPERTY_TABLE",
    destination_project_dataset_table=f"housingdataanalysis.HousingData.Property",
    bucket='transformed_data17',
    source_objects=[f"property_data.csv"],
    source_format='CSV',
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

create_sale_table = GCSToBigQueryOperator(
    task_id=f"CREATE_SALE_TABLE",
    destination_project_dataset_table=f"housingdataanalysis.HousingData.Sale",
    bucket='transformed_data17',
    source_objects=[f"sale_data.csv"],
    source_format='CSV',
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

create_tax_table = GCSToBigQueryOperator(
    task_id=f"CREATE_TAX_TABLE",
    destination_project_dataset_table=f"housingdataanalysis.HousingData.Tax",
    bucket='transformed_data17',
    source_objects=[f"tax_data.csv"],
    source_format='CSV',
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

create_value_assessment_table = GCSToBigQueryOperator(
    task_id=f"CREATE_VALUE_ASSESSMENT_TABLE",
    destination_project_dataset_table=f"housingdataanalysis.HousingData.ValueAssessment",
    bucket='transformed_data17',
    source_objects=[f"value_assessment_data.csv"],
    source_format='CSV',
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

create_property_details_table = GCSToBigQueryOperator(
    task_id=f"CREATE_PROPERTY_DETAILS_TABLE",
    destination_project_dataset_table=f"housingdataanalysis.HousingData.PropertyDetails",
    bucket='transformed_data17',
    source_objects=[f"property_details_data.csv"],
    source_format='CSV',
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

create_ownership_table = GCSToBigQueryOperator(
    task_id=f"CREATE_OWNERSHIP_TABLE",
    destination_project_dataset_table=f"housingdataanalysis.HousingData.Ownership",
    bucket='transformed_data17',
    source_objects=[f"ownership_data.csv"],
    source_format='CSV',
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

end = BashOperator(task_id="END", bash_command="echo end", dag=dag)

start >> extract_data_from_api >> merge_data_from_landing_zone >> clean_data >> transform_data >> [create_property_table, create_sale_table, create_tax_table, create_value_assessment_table, create_property_details_table, create_ownership_table] >> end