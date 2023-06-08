from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from datetime import datetime

default_args = {
	'owner' : 'v0.0.2/gazuaa',
	'depends_on_past' : False,
	'start_date' : datetime(2023,6,7),}

dag = DAG('kosdaqCode_dag', default_args = default_args, schedule_interval = '@once')

def load_kosdaq_raw():
	# kosdaq 종목번호 load 후 column명 붙여주기
	import pandas as pd

	url = 'https://kind.krx.co.kr/corpgeneral/corpList.do'
	kosdaq_code = pd.read_html(url + "?method=download&marketType=kosdaqMkt")[0]
	kosdaq_code = kosdaq_code[['회사명', '종목코드']]
	print(kosdaq_code)
	kosdaq_code.to_csv('/opt/airflow/tmp/kosdaq_raw.csv', index = False)

def convert_kosdaq_code():
	import io 
	with open('/opt/airflow/tmp/kosdaq_raw.csv', 'r') as input_file:
		lines = input_file.readlines()[1:]

	#convert_lines = [f"{line.lstrip(0).zfill(6)}" for line in lines]
	convert_lines =""

	for line in lines:
		tmp_companyName = line.split(',')[0]
		tmp_companyCode = str(line.split(',')[1]).strip().zfill(6) + ".KQ"
		tmp_line = tmp_companyName + ', ' + tmp_companyCode + '\n'
		convert_lines += tmp_line

	with open("/opt/airflow/tmp/convert_tmp.csv", "w") as output_file:
		output_file.writelines(convert_lines)

def load_kosdaq_code():
	import mysql.connector,io 

	conn = mysql.connector.connect(user='stock', password= '1234', host='192.168.90.128', database = 'stock', port = '3306', auth_plugin='mysql_native_password')
	
	with open('/opt/airflow/tmp/convert_tmp.csv', 'r') as input_file:
		lines = input_file.readlines()

		cursor = conn.cursor()
		for line in lines:
			tmp_companyName, tmp_companyCode = line.strip().split(',')
			query = "insert into kosdaq_code (company_name, company_code) values(\"%s\", \"%s\")" %(tmp_companyName, tmp_companyCode)
			cursor.execute(query)
			conn.commit()

	conn.close()

	
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

start = EmptyOperator(
	task_id = 'start',
	dag = dag)	

start_noti = gen_noti("start_dag_noti", "시작")

get_rawCode = PythonOperator(
	task_id = 'get_kosdaq_raw',
	python_callable = load_kosdaq_raw,
	dag = dag)

convert_rawCode = PythonOperator(
	task_id = 'convert_kosdaq_code',
	python_callable = convert_kosdaq_code,
	dag = dag)

load_data_toServer = BashOperator(
	task_id = 'send_final_data',
	bash_command = "scp /opt/airflow/tmp/convert_tmp.csv \
					yoda@192.168.90.128:/home/yoda/stock/tmp/kosdaq.csv",
	dag = dag)

load_data_toDB = PythonOperator(
	task_id = 'load_data_DB',
	python_callable = load_kosdaq_code,
	dag = dag)

delete_tmpData = BashOperator(
	task_id = 'delete_tmp_data',
	bash_command = "rm -rf /opt/airflow/tmp/*",
	dag = dag
)

done_noti = gen_noti("all_done_noti", "완료")

finish = EmptyOperator(
	task_id = 'finish',
	dag = dag)

start >> start_noti >> get_rawCode >> convert_rawCode >> load_data_toDB >> done_noti >> finish
# start >> start_noti >> get_rawCode >> done_noti >> finish
