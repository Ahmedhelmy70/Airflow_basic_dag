from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from random import randint

def print_hello():
    print("Hello, Ahmed Helmy")

def random_num():
    n = randint(1, 10)
    with open("/opt/airflow/shared/random.txt", 'w') as f:
        f.write(str(n))

with DAG(
        dag_id= "Airflow_depi",
        start_date= datetime(2025, 10, 1),
        schedule_interval= timedelta(minutes=1)
        ) as dag:
    print_date = BashOperator(
            task_id = "print_date",
            bash_command = 'date'
            )
    print_welcome = PythonOperator(
            task_id = "print_hello",
            python_callable= print_hello
            )
    generate_random = PythonOperator(
            task_id = "generate_random",
            python_callable = random_num
            )
print_date >> print_welcome >> generate_random
