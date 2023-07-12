##################  At First  ##################
# HDFS내에 실행 날짜에 해당하는 디렉토리가 있는지 체크하는 함수
@app.get("/checkHDFS-folder/data_category={data_category}/exe_year={exe_year}/exe_period={exe_period}")
async def check_folder(data_category: str, exe_year: str, exe_period: str):
    import sys
    from pywebhdfs.webhdfs import PyWebHdfsClient
    hdfs = PyWebHdfsClient(host='192.168.90.128', port='9870', user_name='yoda')
    base_path = '/user/stock/raw/price_data'

    if data_category in ['KOSPI-day', 'KOSDAQ-day'] or data_category in ['KOSPI-minute', 'KOSDAQ-minute']:
        market_name, duration = data_category.split('-')
        folder_path = f"{base_path}/{market_name}/{duration}/{exe_year}/{exe_period}"
        response = None
        try:
            response = hdfs.list_dir(folder_path)
        except:
            pass

        if response is None:
            return "1"
        else:
            return {"status": "success"}
    else:
        return "1"


##################  IF checkHDFS-folder is True  ##################
# checkHDFS-folder에서 True일 경우 이 함수를 사용.
# data_category에 따라 hdfs에서 데이터를 가져옴.
# data_category -> KOSPI/MINUTE, KOSPI/DAY, KOSDAQ/MINUTE, KOSDAQ/DAY
@app.get("/from/hdfs/data_category={data_category}/exe_day={exe_day}")
async def download_from_hdfs(data_category: str, exe_day: str):
    import subprocess

    data_category = data_category.upper()
    hdfs_base_path = "/user/stock/raw"
    local_base_path = "/home/yoda/stock/raw"

    year, month, day = exe_day.split('-')

    if data_category in ['KOSPI-DAY', 'KOSDAQ-DAY']:
        category = data_category.split('-')[0]
        duration = data_category.split('-')[1].lower()

        if month in ['01', '02', '03']:
            path_suffix = "1"
        elif month in ['04', '05', '06']:
            path_suffix = "2"
        elif month in ['07', '08', '09']:
            path_suffix = "3"
        elif month in ['10', '11', '12']:
            path_suffix = "4"
        else:
            return "Invalid month"

        additional_path = f"/price_data/{category}/{duration}/{year}"
        local_path = f"{local_base_path}{additional_path}/{path_suffix}"
        hdfs_path = f"{hdfs_base_path}{additional_path}/{path_suffix}"

    elif data_category in ['KOSPI-MINUTE', 'KOSDAQ-MINUTE']:
        category = data_category.split('-')[0]
        duration = data_category.split('-')[1].lower()

        additional_path = f"/price_data/{category}/{duration}/{year}"
        local_path = f"{local_base_path}{additional_path}/{month}"
        hdfs_path = f"{hdfs_base_path}{additional_path}/{month}"

        command = f"hdfs dfs -copyToLocal -r {hdfs_path} {local_path}"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
    else:
        return "Invalid data category"

    return result.stdout


# 분봉 데이터, HDFS에서 다운받은 기존데이터 & 당일에 yFinance로 내려받은 데이터 concat하는 함수
# check hdfs folder에서 True면 이 function(concat)을 한 후에 -> put hdfs function
@app.get("concat-min-data/data_category={data_category}/exe_day={exe_day}")
async def concat_min_data(data_category:str, exe_day:str):
    year, month, day = exe.day.split('-')
    start_date = exe_day
    next_date = datetime.strftime(start_date + timedelta(days=1), "%Y-%m-%d")

    local_base_path = "/home/yoda/stock"
    market_name , duration = data_category.split('/')
    duration = duration.lower()
    additional_path = f"/price_data/{market_name}/{duration}/{year}/{month}{day}"

    import mysql.connector

    conn = mysql.connector.connect(user='stock', password='1234', host='192.168.90.128', database='stock', port='3306', auth_plugin='mysql_native_password')
    cursor = conn.cursor()
    query = 'select company_code from kospi_code'
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    ticker_list = [row[0] for row in result]

    for num in range(len(ticker_list)):
        ticker_no = ticker_list[num]
        info_num = ticker_no.split('.')[0]

        local_data_path = f"{local_base_path}{additional_path}/{info_num}.csv"
        copied_hdfs_data_path = f"/home/yoda/stock/raw/price_data/{market_name}/{duration}/{year}/{month}/{info_num}.csv"
        
        local_data = pd.read_csv(local_data_path)
        hdfs_data = pd.read_csv(copied_hdfs_data_path)
        combined_df = pd.concat([hdfs_data, local_data], ignore_index=True)
        combined_df.to_csv(copied_hdfs_data_path)

    done_file = f"/home/yoda/stock/raw/price_data/{market_name}/{duration}/{year}/{month}/DONE"
    open(done_file, "w").close()


# 일봉 데이터, HDFS에서 다운받은 기존데이터 & 당일에 yFinance로 내려받은 데이터 concat하는 함수
# check hdfs folder에서 True면 이 function(concat)을 한 후에 -> put hdfs function
@app.get("concat-day-data/data_category={data_category}/exe_day={exe_day}")
async def concat_day_data(data_category:str, exe_day:str):
    year, month, day = exe.day.split('-')
    start_date = exe_day
    next_date = datetime.strftime(start_date + timedelta(days=1), "%Y-%m-%d")

    local_base_path = "/home/yoda/stock"
    market_name , duration = data_category.split('/')
    duration = duration.lower()
    additional_path = f"/price_data/{market_name}/{duration}/{year}/{month}{day}"

    import mysql.connector

    conn = mysql.connector.connect(user='stock', password='1234', host='192.168.90.128', database='stock', port='3306', auth_plugin='mysql_native_password')
    cursor = conn.cursor()
    query = 'select company_code from kospi_code'
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    ticker_list = [row[0] for row in result]

    if month in ['01', '02', '03']:
        path_suffix = "1"
    elif month in ['04', '05', '06']:
        path_suffix = "2"
    elif month in ['07', '08', '09']:
        path_suffix = "3"
    elif month in ['10', '11', '12']:
        path_suffix = "4"
    else:
        return "Invalid month"

    for num in range(len(ticker_list)):
        ticker_no = ticker_list[num]
        info_num = ticker_no.split('.')[0]

        local_data_path = f"{local_base_path}{additional_path}/{info_num}.csv"
        copied_hdfs_data_path = f"/home/yoda/stock/raw/price_data/{market_name}/{duration}/{year}/{path_suffix}/{info_num}.csv"
        
        local_data = pd.read_csv(local_data_path)
        hdfs_data = pd.read_csv(copied_hdfs_data_path)
        combined_df = pd.concat([hdfs_data, local_data], ignore_index=True)
        combined_df.to_csv(copied_hdfs_data_path)

    done_file = f"/home/yoda/stock/raw/price_data/{market_name}/{duration}/{year}/{month}/DONE"
    open(done_file, "w").close()


##################  At The END  ##################
# 맨 마지막에 hdfs에 data를 load하는 함수
# check hdfs folder에서 false면 get_dayData 실행 후 이 function을 실행 -> 보통 month의 첫 개장일 일 것이다.
@app.get("local_path_to_hdfs_path/data_category={data_category}/exe_day={exe_day}")
async def local_path_to_hdfs_path(data_category:str, exe_day:str):
    import subprocess

    year, month, day = exe.day.split('-')
    start_date = exe_day
    next_date = datetime.strftime(start_date + timedelta(days=1), "%Y-%m-%d")

    market_name , duration = data_category.split('-')
    duration = duration.lower()

    local_base_path = "/home/yoda/stock"
    additional_path = f"/price_data/{market_name}/{duration}/{year}/{month}{day}"
    local_data_path = f"{local_base_path}{additional_path}"

    copied_hdfs_base_path = f"/home/yoda/stock/raw/price_data/{market_name}/{duration}/{year}/{month}"

    command = f"cp -r {local_data_path} {copied_hdfs_base_path}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)






