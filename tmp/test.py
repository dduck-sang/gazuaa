from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz, pendulum

local_tz = pendulum.timezone("Asia/Seoul")

default_args ={
	'owner' : 'yoda_jei',
	'depends_on_past' : True,
	'start_date' : datetime(2023, 6, 1, tzinfo=local_tz)
	#'start_date' : datetime(2023,6,1, tzinfo=pytz.timezone('Asia/Seoul'))
}

dag = DAG(
	dag_id='tttt', 
	default_args = default_args, 
	# schedule_interval ='0 10 * * 1-5'
	schedule_interval ='0 11 * * *',
	user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")}
)

test1 = BashOperator(
	task_id = 'test1',
	bash_command = """
	echo "execution_date => {{execution_date}}"
	echo "ds => {{ds}}"
	echo "logical_date => {{logical_date}}"
	echo "local_dt(execution_date) => {{local_dt(execution_date)}}"
	""",
	dag = dag)

test1