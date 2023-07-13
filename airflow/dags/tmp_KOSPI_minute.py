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
dag = DAG('get_kospi_min_ex', default_args = default_args, schedule_interval ='0 16 * * 1-5')

# variables
param_date = "{{ next_execution_date.strftime('%Y-%m-%d') }}"
year, month, day = param_date.split('-')

def gen_noti(name: str, stats: str, role: str):
    #line noti 보내는 operator 생성 함수
    cmd = """
        curl -X POST -H 'Authorization: Bearer {{var.value.BEARER_TOKEN_YODA}}' \
        -F 'message= \n DAG이름 : {{dag.dag_id}} stat!' \
        https://notify-api.line.me/api/notify
    """

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
        trigger_rule = role,
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

# check_hdfs_dir = curl_fastAPI1("check_hdfs_dir",f"192.168.90.128:1212/checkHDFS?data_category=KOSPI/minute&exe_year={year}&exe_period={month}")
check_hdfs_dir = BashOperator(
    task_id = 'check_hdfs_dir',
    bash_command=f"""
        RETURN_VALUE=$(curl 192.168.90.128:1212/checkHDFS/data_category=KOSPI-minute&exe_year={year}/exe_period={month})
        if [[ $RETURN_VALUE == '1' ]]; then
            exit 999
        fi
    """,
    dag = dag
    )

local_path_to_hdfs_path = curl_fastAPI("move_data_to_hdfs_dir",f"192.168.90.128:1212/local_path_to_hdfs_path/data_category=KOSPI-minute/exe_day={{ next_execution_date.strftime('%Y-%m-%d') }}", "none_failed_min_one_success")

move_to_local = curl_fastAPI("download_from_hdfs","192.168.90.128:1212/from/hdfs/data_category=KOSPI-minute/exe_day='{{ next_execution_date.strftime('%Y-%m-%d') }}'", "all_success")

get_kospi_data1 = curl_fastAPI("get_KOSPI_DATA1","192.168.90.128:1212/stock-price/kospi-all/day='{{ next_execution_date.strftime('%Y-%m-%d') }}'","all_success")
get_kospi_data2 = curl_fastAPI("get_KOSPI_DATA2","192.168.90.128:1212/stock-price/kospi-all/day='{{ next_execution_date.strftime('%Y-%m-%d') }}'", "all_failed")

concat_min_data = curl_fastAPI("concat_min_data","192.168.90.128:1212/concat-min-data/data_category=KOSPI-minute/exe_day='{{ next_execution_date.strftime('%Y-%m-%d') }}'", "all_success")

check_done_flag = BashOperator(
    task_id = 'check_done_file',
    bash_command="""
        RETURN_VALUE=$(curl 192.168.90.128:1212/check/hdfs/kospi-minute/{{next_execution_date.strftime('%Y-%m-%d')}})
        if [[ $RETURN_VALUE == '1' ]]; then
            exit 999
        fi
    """,
    dag = dag,
    trigger_rule = 'all_done'
    )

# finish_noti1 = gen_noti("finish_dag_noti1", "종료", "all_success")
# finish_noti2 = gen_noti("finish_dag_noti2", "휴장일","one_failed")

put_hdfs = EmptyOperator(
    task_id = 'put_hdfs',
    trigger_rule = 'all_done',
    dag = dag)

finish = EmptyOperator(
    task_id = 'finish',
    trigger_rule = 'all_done',
    dag = dag)


# task dependencies
start >> start_noti >> market_calendar >> check_hdfs_dir >> get_kospi_data2 >> local_path_to_hdfs_path >> check_done_flag >> put_hdfs #>>finish_noti1
start >> start_noti >> market_calendar >> check_hdfs_dir >> move_to_local >> get_kospi_data1 >> concat_min_data >> check_done_flag >> put_hdfs #>> finish_noti1
start >> start_noti >> market_calendar #>> finish_noti2
[market_calendar, put_hdfs] >> finish
