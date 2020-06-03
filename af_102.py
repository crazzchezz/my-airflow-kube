from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator
import random

args = {
    "owner": "dennislau",
    "start_date": days_ago(1)
}

dag = DAG(dag_id='simple_dag', default_args=args, schedule_interval=None)

def run_this_func(**context):
    print("hellow world!")

def always_fail(**context):
    raise Exception('Exception')

def random_fail(**context): 
    if random.random() > 0.7:
        raise Exception('random exception')
    else:
        print('passed!')

with dag:
    task_1 = PythonOperator(task_id='run_this_1', python_callable=random_fail, provide_context=True, retries=10, retry_delay=timedelta(seconds=2))
    task_2 = PythonOperator(task_id='run_this_2', python_callable=run_this_func, provide_context=True)
    task_1 >> task_2

