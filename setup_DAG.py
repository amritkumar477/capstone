import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import google.auth
import google.auth.transport.requests





credentials, project_id = google.auth.default(scopes=['https://storage.cloud.google.com/amrit-bucket/fractal1a-64abd9ab1332.json'])
authed_session = google.auth.transport.requests.AuthorizedSession(credentials)
location = 'asia-south2' 


default_args = {
        'retries': 1,
        'owner' : 'airflow',
        'execution_timeout' : timedelta(seconds=300),
        'start_date' : airflow.utils.dates.days_ago(1)
}



dag = DAG(
        dag_id='amrit-dag',
        default_args=default_args,
        #dagrun_timeout=timedelta(minutes=20),
        schedule_interval='@once', 
        catchup=False,
    )

# Step 4: Creating task
# Creating tasks

start = DummyOperator(task_id = 'start',
                      dag = dag
                        )



sql_to_bq = BashOperator(
                task_id = 'sql_to_bq',

                bash_command =  "gsutil cat gs://ashu_bucket/incremental.py",
                dag = dag) 
    
end = DummyOperator(task_id = 'end',
                     dag = dag)

start >> sql_to_bq >>end 
