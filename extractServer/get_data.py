from fastapi import FastAPI, Path
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import FinanceDataReader as fdr
import os, pytz, logging, zipfile

app = FastAPI()

korean_tz = pytz.timezone('Asia/Seoul')
current_date = datetime.now(tz=korean_tz).strftime("%y%m%d")

log_file_path = f"/home/yoda/stock/price_data/logs/{current_date}.log"

logging.basicConfig(
    filename=log_file_path,)


# 환율 정보 수집기
async def get_currency(start_date:str,end_date:str):

    currency_list = ['USDEUR=X', 'USDGBP=X', 'USDJPY=X', 'USDCHF=X', 'USDCAD=X', \
    'USDAUD=X', 'USDNZD=X', 'USDKRW=X', 'EURGBP=X', 'EURJPY=X', 'EURCHF=X', 'EURCAD=X',\
    'EURAUD=X', 'EURNZD=X', 'EURKRW=X', 'GBPJPY=X', 'GBPCHF=X', 'GBPCAD=X', 'GBPAUD=X',\
    'GBPNZD=X', 'GBPKRW=X', 'JPYCHF=X', 'JPYCAD=X', 'JPYAUD=X', 'JPYNZD=X', 'JPYKRW=X',\
    'CHFCAD=X', 'CHFAUD=X', 'CHFNZD=X', 'CHFKRW=X', 'CADAUD=X', 'CADNZD=X', 'CADKRW=X',\
    'AUDNZD=X', 'AUDKRW=X', 'NZDKRW=X']

    current_time = datetime.strptime(start_date, "%Y-%m-%d")

    this_year = datetime.strftime(current_time, "%Y")
    this_date = datetime.strftime(current_time, "%m%d")

    tmp_df = pd.DataFrame()
    tmp_df["currency"] = ""

    for i in currency_list:
        curr_df = yf.download(i, start=start_date, end=end_date)
        curr_df["currency"] = i
        tmp_df = pd.concat([tmp_df, curr_df])

    address = r"/home/yoda/stock/currency"
    directory = os.path.join(address, this_year, this_date)

    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, "currency.csv")
    tmp_df.to_csv(path_or_buf=file_path)

@app.get("/currency/start-day={start_date}/finish-day={end_date}")
async def get_currency_info_route(start_date:str, end_date:str):
    await get_currency(start_date, end_date)

# 회사 정보 수집기
async def get_companyInfo():

    current_time = datetime.now() - timedelta(days=1)

    this_year = datetime.strftime(current_time, "%Y")
    this_month = datetime.strftime(current_time, "%m")

    url = 'https://kind.krx.co.kr/corpgeneral/corpList.do'
    kospi_code = pd.read_html(url + "?method=download&marketType=stockMkt")
    kosdaq_code = pd.read_html(url + "?method=download&marketType=kosdaqMkt")

    kospi_df = kospi_code[0]
    kosdaq_df = kosdaq_code[0]

    combined_df = pd.concat([kospi_df, kosdaq_df]).reset_index(drop=True)

    # tmp path to download csv
    address = r"/home/yoda/stock/company_info"
    directory = os.path.join(address, this_year, this_month)

    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, "company_info.csv")
    combined_df.to_csv(path_or_buf=file_path)

@app.get("/company")
async def get_company_info_route():
    await get_companyInfo()

# kosdaq 당일 분봉 수집기
@app.get("/stock-price/kosdaq-all/day={exe_day}")
async def list_ticker(exe_day: str):

    logging.info("당일 코스닥 [본봉] 종목 수집 실행")

    import mysql.connector

    conn = mysql.connector.connect(user='stock', password= '1234', host='192.168.90.128', database = 'stock', port = '3306', auth_plugin='mysql_native_password')

    start_date = exe_day
    market_name = 'KOSDAQ'

    cursor = conn.cursor()
    query = 'select company_code from kosdaq_code'
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    ticker_list = [row[0] for row in result]

    for num in range(len(ticker_list)):
        ticker_no = ticker_list[num]
        dataPeriod ="minute"

        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        next_date = datetime.strftime(start_datetime + timedelta(days=1), "%Y-%m-%d")

        now_year = start_date.split('-')[0]
        info_num = ticker_no.strip().split('.')[0]
        to_date = start_date[5:7] + start_date[8:10]

        data = yf.download(tickers=ticker_no, start=start_date, end=next_date, interval='1m')

        file_path = "/home/yoda/stock/price_data/{}/{}/{}/{}/{}.csv".format(market_name, dataPeriod,now_year, to_date, info_num)
        #/home/yoda/stock/price_data/KOSPI/minute/2023/0613/005930.csv
        directory = os.path.dirname(file_path)

        if not os.path.exists(directory):
            os.makedirs(directory)
        data.to_csv(file_path)

    done_file = "/home/yoda/stock/price_data/KOSDAQ/minute/2023/{}/DONE".format(to_date)
    open(done_file, "w").close()

# kospi 당일 분봉 수집기
@app.get("/stock-price/kospi-all/day={exe_day}")
async def get_dayData(exe_day: str):

    logging.info("당일 코스피 [분봉] 종목 수집 실행")

    import mysql.connector

    conn = mysql.connector.connect(user='stock', password= '1234', host='192.168.90.128', database = 'stock', port = '3306', auth_plugin='mysql_native_password')

    start_date = exe_day
    market_name = 'KOSPI'

    cursor = conn.cursor()
    query = 'select company_code from kospi_code'
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    ticker_list = [row[0] for row in result]

    for num in range(len(ticker_list)):
        ticker_no = ticker_list[num]
        dataPeriod ="minute"

        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        next_date = datetime.strftime(start_datetime + timedelta(days=1), "%Y-%m-%d")

        now_year = start_date.split('-')[0]
        info_num = ticker_no.split('.')[0]
        to_date = start_date[5:7] + start_date[8:10]

        data = yf.download(tickers=ticker_no, start=start_date, end=next_date, interval='1m')
        #/home/yoda/stock/price_data/KOSPI/minute/2023/0613/005930.csv
        file_path = "/home/yoda/stock/price_data/{}/{}/{}/{}/{}.csv".format(market_name, dataPeriod,now_year, to_date, info_num)
        directory = os.path.dirname(file_path)

        if not os.path.exists(directory):
            os.makedirs(directory)

        data.to_csv(file_path)

    done_file = "/home/yoda/stock/price_data/KOSPI/minute/2023/{}/DONE".format(to_date)
    open(done_file, "w").close()

# kospi 당일 일봉 수집기
async def get_day_KSprice(exe_day:str):
    # 함수들
    logging.info("당일 코스피 [일봉] 종목 수집 실행")

    import mysql.connector

    conn = mysql.connector.connect(user='stock', password= '1234', host='192.168.90.128', database = 'stock', port = '3306', auth_plugin='mysql_native_password')

    start_date = exe_day
    market_name = 'KOSPI'

    cursor = conn.cursor()
    query = 'select company_code from kospi_code'
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    ticker_list = [row[0] for row in result]

    for num in range(len(ticker_list)):
        ticker_no = ticker_list[num]
        dataPeriod ="day"

        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        next_date = datetime.strftime(start_datetime + timedelta(days=1), "%Y-%m-%d")

        now_year = start_date.split('-')[0]
        info_num = ticker_no.split('.')[0]
        to_date = start_date[5:7] + start_date[8:10]

        data = yf.download(tickers=ticker_no, start=start_date, end=next_date, interval='1d')
        #/home/yoda/stock/price_data/KOSPI/minute/2023/0613/005930.csv
        file_path = "/home/yoda/stock/price_data/{}/{}/{}/{}/{}.csv".format(market_name, dataPeriod,now_year, to_date, info_num)
        directory = os.path.dirname(file_path)

        if not os.path.exists(directory):
            os.makedirs(directory)

        data.to_csv(file_path)

    done_file = "/home/yoda/stock/price_data/KOSPI/minute/2023/{}/DONE".format(to_date)
    open(done_file, "w").close()

@app.get("stock-price/kospi-once/day={exe_day}")
async def get_kospi_onceData(exe_day: str):
    await get_day_KSprice(exe_day)

# kosdaq 당일 일봉 수집기
async def get_day_KQprice(exe_day:str):
    # 함수들
    logging.info("당일 코스닥 [일봉] 종목 수집 실행")

    import mysql.connector

    conn = mysql.connector.connect(user='stock', password= '1234', host='192.168.90.128', database = 'stock', port = '3306', auth_plugin='mysql_native_password')

    start_date = exe_day
    market_name = 'KOSPI'

    cursor = conn.cursor()
    query = 'select company_code from kospi_code'
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    ticker_list = [row[0] for row in result]

    for num in range(len(ticker_list)):
        ticker_no = ticker_list[num]
        dataPeriod ="day"

        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        next_date = datetime.strftime(start_datetime + timedelta(days=1), "%Y-%m-%d")

        now_year = start_date.split('-')[0]
        info_num = ticker_no.split('.')[0]
        to_date = start_date[5:7] + start_date[8:10]

        data = yf.download(tickers=ticker_no, start=start_date, end=next_date, interval='1d')
        #/home/yoda/stock/price_data/KOSPI/minute/2023/0613/005930.csv
        file_path = "/home/yoda/stock/price_data/{}/{}/{}/{}/{}.csv".format(market_name, dataPeriod,now_year, to_date, info_num)
        directory = os.path.dirname(file_path)

        if not os.path.exists(directory):
            os.makedirs(directory)

        data.to_csv(file_path)

    done_file = "/home/yoda/stock/price_data/KOSPI/minute/2023/{}/DONE".format(to_date)
    open(done_file, "w").close()

@app.get("stock-price/kospi-once/day={exe_day}")
async def get_kosdaq_onceData(exe_day: str):
    await et_day_KQprice(exe_day)

# dart 공시코드 binary 수집코드
@app.get("/dartcode/all")
async def get_dart_code():

    import xml.etree.ElementTree as ET
    import requests

    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    dart_api_key = '65fa08efbb23ba02ccb4959a477579a66bbc5637'

    parmas = {'crtfc_key': dart_api_key }

    response = requests.get(url, parmas)

    if response.status_code == 200:
        with open('/home/yoda/stock/tmp/dart_code.zip', 'wb') as file:
            file.write(response.content)
            print("download compelete")
    else:
        print("failed status code :", response.status_code)

    zip_path = '/home/yoda/stock/tmp/dart_code.zip'
    output_path = '/home/yoda/stock/tmp'

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_path) 

    with open(output_path, 'r', encoding='utf-8') as file:
        xml_data = file.read()

    root = ET.fromstring(xml_data)
    data_list = root.findall('list')

    df_data = []
    for data in data_list:
        corp_code = data.find('corp_code').text
        corp_name = data.find('corp_name').text
        modify_date = data.find('modify_date').text
        df_data.append({'corp_code': corp_code, 'corp_name': corp_name, 'modify_date': modify_date})

    tmp_file = pd.DataFrame(df_data)

    address = r"/home/yoda/stock/company_info"
    directory = os.path.join(address, this_year, this_month)

    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, "dart_code.csv")
    tmp_file.to_csv(path_or_buf=file_path, index=False)

@app.get("/dartcode/db/all")
async def update_dart_code():

    import mysql.connector, csv

    conn = mysql.connector.connect(user='stock', password= '1234', host='192.168.90.128', database = 'stock', port = '3306', auth_plugin='mysql_native_password')

    dartCode_data =[]

    with open("/home/yoda/stock/company_info/dart_code.csv", "r") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            dartCode_data.append(row)

    cursor = conn.cursor()

    for row in dartCode_data:
        name = row[1].encode('cp949')
        dart_code = row[0]

        query = 'update kospi_code set dart_code = %s where company_name = %s'

        cursor.execute(query)

    conn.commit()

    conn.close()
##    