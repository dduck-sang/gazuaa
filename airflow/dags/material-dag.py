from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
	"owner": "230608/woorek",
	"depends_on_past": "False",
	"start_date": datetime(2023, 6, 9),
	"retries": 0
}

dag = DAG('material-dag', default_args=default_args, schedule_interval='@once')

# task Operators
t1 = BashOperator(
	task_id = 'task_1'
	
)