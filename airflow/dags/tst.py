from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
	"owner": "230608/woorek",
	"depends_on_past": "False",
	"start_date": datetime(2023, 6, 8),
	"retries": 0
}


#DAG
dag = DAG('currency-dag', default_args=default_args, schedule_interval='@once')

# define functions to use
def get_currency(date):
	import FinanceDataReader as fdr

	df = fdr.DataReader('USD/KRW', start=date)
	print(df)

# define variables to use
date_used = datetime.now().strftime("%Y-%m-%d")

# task Operators
t1 = PythonOperator(
	task_id = 'task_1',
	provide_context = True,
	python_callable = get_currency,
	op_kwargs = {"date" : date_used},
	dag = dag
)

# Empty Operators
empty_s_t = EmptyOperator(task_id = "start", dag = dag)
empty_e_t = EmptyOperator(task_id = "end", dag = dag)

# Task dependencies
empty_s_t >> t1 >> empty_e_t