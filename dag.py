from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

####### CHANGE THIS ##########
PREFIX = 'yourprefix'
##############################


INGEST_JOB = PREFIX + '-ingest'
ENRICH_JOB = PREFIX + '-enrich'
DAG_NAME   = PREFIX + '_demo_dag'

default_args = {
    'owner': 'cdeuser1',
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2021,1,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
dag = DAG(
    DAG_NAME,
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False
)
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

ingest_task = CDEJobRunOperator(
    task_id='ingest',
    dag=dag,
    job_name=INGEST_JOB
)

enrich_task = CDEJobRunOperator(
    task_id='enrich',
    dag=dag,
    job_name=ENRICH_JOB
)

start >> ingest_task >> enrich_task >> end
