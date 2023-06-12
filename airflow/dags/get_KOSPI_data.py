from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz

default_args ={
	'owner' : 'yoda_jei',
	'depends_on_past' : False,
	'start_date' : datetime(2023, 6, 7, tzinfo=pytz.timezone('Asia/Seoul'))
}

dag = DAG('get_kospoi_data', default_args = default_args, schedule_interval ='0 16 * * 1-5')

param_date = "{{ execution_date.strftime('%Y-%m-%d') }}"

def gen_noti(name: str, stats: str):
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
 		dag=dag
  	)

	return bash_task

def get_priceData(name: str, url : str):
	#line noti 보내는 operator 생성 함수

	curl_cmd = """
		curl link
	"""

	curl_cmd = curl_cmd.replace("link", url)
	curl_cmd = curl_cmd.replace("exe_day", param_date)	

	print(curl_cmd)

	# getData 생성 func.
	bash_task = BashOperator(
		task_id=name,
 		bash_command=curl_cmd,
 		dag=dag
  	)

	return bash_task

start = EmptyOperator(
	task_id = 'start_task',
	dag = dag)

start_noti = gen_noti("start_dag_noti", "시작")

get_kospi_data = get_priceData("get_KOSPI_DATA","192.168.90.128:1212/stock-price/kospi-all/day='exe_day'")

finish_noti = gen_noti("finish_dag_noti", "종료")

finish = EmptyOperator(
	task_id = 'finish',
	dag = dag)


#시작 >> 노티 뿌리기 >> curl로 데이터 받아오기 >> .log로 레포트 수신 >> 데이터 정합성 check >> 끝
# 의문점 .log에 떨어진 log 처리까지는 ok 이거 어떻게 던지냐 그게 관건
# 의문점 데이터 정합성을 check하기 위한 방법? 다운 완료는 DONE 플래그 생성으로 check후 처리 
start >> start_noti >> get_kospi_data >> finish_noti >> finish