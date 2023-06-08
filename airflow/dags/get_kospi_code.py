from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from datetime import datetime

default_args = {
	'owner': 'v0.0.1/gazuaa',
	'depends_on_past' : False,
	'start_date': datetime(2023,1,1),
}

dag = DAG('kospiCode_dag', default_args = default_args, schedule_interval = '@once')

def load_raw_code():
	# 전자공시시스템에서 kospi 종목번호를 load 하여 column 붙이기
	import pandas as pd
	url = 'https://kind.krx.co.kr/corpgeneral/corpList.do'
	kospi_code = pd.read_html(url + "?method=download&marketType=stockMkt")[0]
	kospi_code = kospi_code[['회사명', '종목코드']]

	return kospi_code

def convert_kospi_code(x):
	# 종목번호를 ticker number에 맞게 변환
	tmp_data = str(x)
	return '0' * (6-len(tmp_data)) + tmp_data + '.KS'
	

def apply_kospi_code(**context):
	# ticker number로 변환한 data를 기존 df에 붙여넣기 
	convert_code = context['ti'].xcom_pull(task_ids='get_tmpKospi_code')
	convert_code['종목코드'] = convert_code['종목코드'].apply(convert_kospi_code)
	return convert_code

def load_kospi_code(**context):
	# 변환 완료된 data를 db server에 적재
	import mysql.connector

	final_data = context['ti'].xcom_pull(task_ids='convert_code')
	conn = mysql.connector.connect(user='stock', password= '1234', host='192.168.90.128', database = 'stock', port = '3306', auth_plugin='mysql_native_password')
	print(final_data)
	cursor = conn.cursor()

	for index, row in final_data.iterrows():

		company_name = row['회사명']
		company_code = row['종목코드']
		
		values = (company_name, company_code)
		print(values)
		
		query = "INSERT INTO kospi_code (company_name, company_code) VALUES (\"%s\", \"%s\")" %(values[0], values[1])
		#print(query)
		cursor.execute(query)
		conn.commit()
	conn.close()


first = EmptyOperator(
	task_id = 'start',
	dag = dag
)

start_noti = BashOperator(
	task_id = 'start_noti',
	bash_command = """
		curl -X POST -H 'Authorization: Bearer {{var.value.BEARER_TOKEN_YODA}}' \
		-F 'message= \n DAG이름 : {{dag.dag_id}} 시작!' \
		https://notify-api.line.me/api/notify
	""",
	dag = dag
)

get_rawCode = PythonOperator(
	task_id = 'get_tmpKospi_code',
	python_callable = load_raw_code,
	dag = dag
)

convert_tmpCode = PythonOperator(
	task_id = 'convert_code',
	python_callable = apply_kospi_code,
	dag = dag
)

load_code = PythonOperator(
	task_id = 'load_kospi_code',
	python_callable = load_kospi_code,
	dag = dag
)

finish = EmptyOperator(
	task_id = 'finish',
	dag = dag
)

first >> start_noti >> get_rawCode >> convert_tmpCode >> load_code >> finish 
