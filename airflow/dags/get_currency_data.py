from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pytz, pendulum

KST = pendulum.timezone("Asia/Seoul")

default_args = {
	"owner": "v0.0.5/gazua",
	"depends_on_past": False,
	"start_date": datetime(2023, 6, 1, tzinfo=KST),
	"retries": 0
}


#DAG
dag = DAG('get-currency-dag', default_args=default_args, schedule_interval='0 0 * * *', max_active_runs=2, tags=["수집","환율"])

## define functions to use
# 36개 환율 data 가져오는 func
def get_currency(name:str, url:str):

	curl_cmd = """
		curl link
	"""

	curl_cmd = curl_cmd.replace("link", url)

	bash_task = BashOperator(
		task_id=name,
		bash_command=curl_cmd,
		dag=dag
	)

	return bash_task

def gen_noti(name: str, stats: str, role:str):
	#line noti 보내는 operator 생성 함수
	cmd = """
 		curl -X POST -H 'Authorization: Bearer {{var.value.BEARER_TOKEN_YODA}}' \
 		-F 'message= \n DAG이름 : {{dag.dag_id}} stat!' \
 		https://notify-api.line.me/api/notify
 	"""
	
	cmd = cmd.replace("stat", stats)

	# noti 생성 operator 
	bash_task = BashOperator(
		task_id=name,
 		bash_command=cmd,
 		trigger_rule=role,
 		dag=dag
  	)

	return bash_task

# define variables to use
# start_param = "{{ next_execution_date.strftime('%Y-%m-%d') }}"
# end_param = "{{ next_execution_date.strftime('%Y-%m-%d') }}"

# task Operators
start_noti = gen_noti("start_dag_noti", "시작", "all_success")
get_currency = get_currency("get_currency","192.168.90.128:1212/currency/start-day={{ execution_date.strftime('%Y-%m-%d') }}/finish-day={{ next_execution_date.strftime('%Y-%m-%d') }}")
finish_noti = gen_noti("finish_dag_noti", "종료", "all_success")

# Empty Operators
empty_s_t = EmptyOperator(task_id = "start", dag = dag)
empty_e_t = EmptyOperator(task_id = "end", dag = dag)

# Task dependencies
empty_s_t >> start_noti >> get_currency >> finish_noti >> empty_e_t
