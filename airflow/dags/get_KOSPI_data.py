from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz, pendulum

KST = pendulum.timezone("Asia/Seoul")

default_args ={
	'owner' : 'v0.0.5/gazua',
	'depends_on_past' : True,
	'start_date' : datetime(2023, 5, 15, tzinfo=KST)
	#'start_date' : datetime(2023,6,1, tzinfo=pytz.timezone('Asia/Seoul'))
}

# utc 기준 7 시로 고치면 되긴함
dag = DAG('get_kospoi_data', default_args = default_args, schedule_interval ='0 16 * * 1-5')


#해당 날짜를 execution_date + 1일로 하여서, 받는 paramators 자체를 excution_date가 아닌 다른 걸로 핸들링
param_date = "{{ next_execution_date.strftime('%Y-%m-%d') }}"

def gen_noti(name: str, stats: str, role: str):
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

def get_priceData(name: str, url : str):
	#종목 데이터 get 하는 func.

	curl_cmd = """
		curl link
	"""

	curl_cmd = curl_cmd.replace("link", url)
	#curl_cmd = curl_cmd.replace("exe_day", param_date)

	# getData 생성 func.
	bash_task = BashOperator(
		task_id=name,
 		bash_command=curl_cmd,
 		dag=dag
  	)

	return bash_task

def get_market_closing(exe_date:str):
    import exchange_calendars as xcals
    import sys

    X_KRX = xcals.get_calendar("XKRX")
    o_x = X_KRX.is_session(exe_date)
    if o_x is True:
        return exe_date
    else:
        sys.exit(1)

start = EmptyOperator(
	task_id = 'start_task',
	dag = dag)


start_noti = gen_noti("start_dag_noti", "시작", "all_success")

market_calendar = PythonOperator(
	task_id= "get_holiday",
	python_callable=get_market_closing,
	op_kwargs={"exe_date":param_date},
	dag=dag
	)

get_kospi_data = get_priceData("get_KOSPI_DATA","192.168.90.128:1212/stock-price/kospi-all/day='{{ next_execution_date.strftime('%Y-%m-%d') }}'")
#get_kospi_data = get_priceData("get_KOSPI_DATA", "192.168.90.128:1212/stock-price/kospi-all/day='exe_day'")

finish_noti1 = gen_noti("finish_dag_noti1", "종료", "all_success")
finish_noti2 = gen_noti("finish_dag_noti2", "휴장일","one_failed")

"""finish = EmptyOperator(
	task_id = 'finish',
	dag = dag)"""


# 시작 >> 노티 뿌리기 >> curl로 데이터 받아오기 >> .log로 레포트 수신 >> 데이터 정합성 check >> 끝
# 의문점 .log에 떨어진 log 처리까지는 ok 이거 어떻게 던지냐 그게 관건
# 의문점 데이터 정합성을 check하기 위한 방법? 다운 완료는 DONE 플래그 생성으로 check후 처리 
# DONE_flag는 api server를 둔다. 이유는 다른 부서/회사로 부터 데이터를 받을때, listener server의 권한 문제로 일종의 미들웨어 개념으로 처리하는 하는 방식
# start >> start_noti >> get_kospi_data >> finish_noti >> finish
start >> start_noti >> market_calendar >> get_kospi_data >> finish_noti1
start >> start_noti >> market_calendar >> finish_noti2