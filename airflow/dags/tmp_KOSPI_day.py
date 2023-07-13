from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz, pendulum

KST = pendulum.timezone("Asia/Seoul")

default_args ={
    'owner' : 'v0.0.5/gazua',
    'depends_on_past' : False,
    'start_date' : datetime(2023, 6, 27, tzinfo=KST)
    #'start_date' : datetime(2023,6,1, tzinfo=pytz.timezone('Asia/Seoul'))
}

# utc 기준 7 시로 고치면 되긴함
dag = DAG('get_kospi_day_ex', default_args = default_args,max_active_runs = 2, tags =['수집','KS일봉'], schedule_interval ='0 16 * * 1-5')

# variables
param_date = "{{ next_execution_date.strftime('%Y-%m-%d') }}"
year, month, day = param_date.split('-')
if month in ['01', '02', '03']:
    quarter = "1"
elif month in ['04', '05', '06']:
    quarter = "2"
elif month in ['07', '08', '09']:
    quarter = "3"
elif month in ['10', '11', '12']:
    quarter = "4"
else :
    quarter = None


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

def curl_fastAPI(name: str, url : str, role: str):
    #종목 데이터 get 하는 func.

    curl_cmd = """
        curl link
    """

    curl_cmd = curl_cmd.replace("link", url)

    bash_task = BashOperator(
        task_id=name,
        bash_command=curl_cmd,
        trigger_rule=role,
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

check_hdfs_dir = BashOperator(
    task_id = 'check_hdfs_dir',
    bash_command=f"""
        RETURN_VALUE=$(curl 192.168.90.128:1212/checkHDFS/data_category=KOSPI-day/exe_year={year}/exe_period={quarter})
        if [[ $RETURN_VALUE == '1' ]]; then
            exit 999
        fi
    """,
    dag = dag
    )

local_path_to_hdfs_path = curl_fastAPI("move_data_to_hdfs_dir",f"192.168.90.128:1212/local_path_to_hdfs_path/data_category=KOSPI-day/exe_day={{ next_execution_date.strftime('%Y-%m-%d') }}", "none_failed_min_one_success")

move_to_local = curl_fastAPI("download_from_hdfs","192.168.90.128:1212/from/hdfs/data_category=KOSPI-day/exe_day='{{ next_execution_date.strftime('%Y-%m-%d') }}'", "all_success")

get_kospi_data1 = curl_fastAPI("get_KOSPI_DATA1","192.168.90.128:1212/stock-price/kospi-once/day='{{ next_execution_date.strftime('%Y-%m-%d') }}'","all_success")
get_kospi_data2 = curl_fastAPI("get_KOSPI_DATA2","192.168.90.128:1212/stock-price/kospi-once/day='{{ next_execution_date.strftime('%Y-%m-%d') }}'", "all_failed")
# get_kospi_data = get_priceData("get_KOSPI_DATA","192.168.90.128:1212/stock-price/kospi-once/day='{{ next_execution_date.strftime('%Y-%m-%d') }}'")

concat_day_data = curl_fastAPI("concat_day_data","192.168.90.128:1212/concat-day-data/data_category=KOSPI-day/exe_day='{{ next_execution_date.strftime('%Y-%m-%d') }}'", "all_success")

check_done_flag = BashOperator(
    task_id = 'check_done_file',
    bash_command="""
        RETURN_VALUE=$(curl 192.168.90.128:1212/check/hdfs/KOSPI-day/{{next_execution_date.strftime('%Y-%m-%d')}})
        if [[ $RETURN_VALUE == '1' ]]; then
            exit 999
        fi
    """,
    dag = dag
	)

# load_hdfs_data = BashOperator(
#     task_id = 'load_hdfs',
#     bash_command = "curl 192.168.90.128:1212/hdfs/kospi-day/{{next_execution_date}}",
#     dag = dag)

# finish_noti1 = gen_noti("finish_dag_noti1", "종료", "all_success")
# finish_noti2 = gen_noti("finish_dag_noti2", "휴장일","one_failed")
upload_to_hdfs = curl_fastAPI("upload_to_hdfs","192.168.90.128:1212/hdfs/KOSPI-minute/'{{ next_execution_date.strftime('%Y-%m-%d') }}'","all_done")


finish = EmptyOperator(
    task_id = 'finish',
    trigger_rule = 'all_done',
    dag = dag)


# task dependencies
start >> start_noti >> market_calendar >> check_hdfs_dir >> get_kospi_data2 >> local_path_to_hdfs_path >> check_done_flag >> upload_to_hdfs #>>finish_noti1
start >> start_noti >> market_calendar >> check_hdfs_dir >> move_to_local >> get_kospi_data1 >> concat_day_data >> check_done_flag >> upload_to_hdfs #>> finish_noti1
start >> start_noti >> market_calendar #>> finish_noti2
[market_calendar, upload_to_hdfs] >> finish
