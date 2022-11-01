from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pandas

count=0 

def vai_somando():
    global count
    count =+ 1

def e_valido():
    global count
    if count > 2:
        return 'grande'
    return 'pequeno'


with DAG('tutorial_dage', start_date = datetime(2022,11,1), schedule = '@hourly', catchup=False) as dag:
    
    vai_somando = PythonOperator(
        task_id = 'vai_somando',
        python_callable = vai_somando
    )

    e_valido = BranchPythonOperator(  #escolhe pra que lado a função vai
        task_id = 'e_valido',
        python_callable = e_valido
    )

    grande = BashOperator(
        task_id = 'grande',
        bash_command = "echo 'grande'"
    )

    pequeno = BashOperator(
        task_id = 'pequeno',
        bash_command = "echo 'pequeno'"
    )


    vai_somando >> e_valido >> [pequeno, grande]