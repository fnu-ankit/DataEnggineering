from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain

default_args={'retries':3}

def print_a():
    print("hi from task a")

def print_b():
    print("hi from task b")

def print_c():
    print("hi from task c")

def print_d():
    print("hi from task d")

def print_e():
    print("hi from task e")

with DAG('my_dag', start_date=datetime(2024,1,1), description='Simple DAG', tags=['Data_Engg']
         ,schedule='@daily', catchup=False) as dag:
    
    task_a = PythonOperator(task_id='task_a', python_callable=print_a)
    task_b = PythonOperator(task_id='task_b', python_callable=print_b)
    task_c = PythonOperator(task_id='task_c', python_callable=print_c)
    task_d = PythonOperator(task_id='task_d', python_callable=print_d)
    task_e = PythonOperator(task_id='task_e', python_callable=print_e)
    
    
    chain(task_a, [task_b, task_c], [task_d, task_e])
