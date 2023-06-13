from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pytz, pendulum

KST = pendulum.timezone("Asia/Seoul")

default_args = {
	"owner": "230608/woorek",
	"depends_on_past": "False",
	"start_date": datetime(2023, 6, 8, tzinfo=KST),
	"retries": 0
}


#DAG
dag = DAG('get-currency-dag', default_args=default_args, schedule_interval='@once')

# define functions to use
def get_currency(date: str):
	import FinanceDataReader as fdr

	df = fdr.DataReader('USD/KRW', start=date)
	print(df)

# define variables to use
param_date = "{{ next_execution_date.strftime('%Y-%m-%d') }}"

# task Operators
t1 = PythonOperator(
	task_id = 'task_1',
	# provide_context = True,
	python_callable = get_currency,
	op_kwargs = {"date" : date_used},
	dag = dag
)

# Empty Operators
empty_s_t = EmptyOperator(task_id = "start", dag = dag)
empty_e_t = EmptyOperator(task_id = "end", dag = dag)

# Task dependencies
empty_s_t >> t1 >> empty_e_t