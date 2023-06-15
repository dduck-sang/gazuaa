from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from datetime import datetime
import pendulum

KST = pendulum.timezone('Asia/Seoul')

default_args ={
	'owner': 'v0.0.6/gazuaa',
	'depends_on_past' : False,
	'start_date': datetime(2023,1,1, tzinfo=KST)}

dag = DAG('get_company_info', default_args = default_args, max_active_runs= 1,tags=['수집','기업정보'] schedule_interval= '0 10 1 * *')

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

def get_compnayInfo(name: str, url : str):
    #종목 데이터 get 하는 func.

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

first = EmptyOperator(
	task_id = 'start_task',
	dag =dag)

start_noti = gen_noti("start_task_noti", "시작", "all_success")

get_rawData = get_compnayInfo("get_company_info", "192.168.90.128:1212/company")

finish_noti = gen_noti("done_task_noti", "종료", "all_success")

finish = EmptyOperator(
	task_id = 'finish_task',
	dag =dag)

first >> start_noti >> get_rawData >> finish_noti >> finish

