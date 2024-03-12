from airflow.decorators import dag
from airflow.utils.helpers import chain
from datetime import datetime

@dag(start_date=datetime(2024,1,1), description='taskflow API dag', tags=['Data_Engg']
     , schedule='@daily', catchup=False)

def my_taskflow_dag():
    @task
    def print_a():
        print("hi from task a")
    
    @task
    def print_b():
        print("hi from task b")

    @task
    def print_c():
        print("hi from task c")
    
    @task
    def print_d():
        print("hi from task d")

    @task
    def print_e():
        print("hi from task e")

    print_a() >> print_b() >> print_c() >> print_d() >> print_e()
    
my_taskflow_dag()
