from fastapi import FastAPI, Path
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import os, pytz, logging

app = FastAPI()

korean_tz = pytz.timezone('Asia/Seoul')
current_date = datetime.now(tz=korean_tz).strftime("%y%m%d")

log_file_path = f"/home/yoda/stock/price_data/logs/{current_date}.log"

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

@app.get("/stock-price/kospi-all/day={exe_day}")
async def list_ticker(exe_day: str):


    logging.info("당일 코스피 종목 수집 실행")

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

        file_path = "/home/yoda/stock/price_data/{}/{}/{}/{}/{}.csv".format(market_name, dataPeriod,now_year, to_date, info_num)
        directory = os.path.dirname(file_path)

        if not os.path.exists(directory):
            os.makedirs(directory)

        data.to_csv(file_path)

    done_file = "/home/yoda/stock/price_data/KOSPI/minute/2023/{}/DONE".format(to_date)
    open(done_file, "w").close()

@app.get("/stock-price/kosdaq-all/day={exe_day}")
async def list_ticker(exe_day: str):

    logging.info("당일 코스닥 종목 수집 실행")

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
        file_name = start_date[5:7] + start_date[8:10]

        data = yf.download(tickers=ticker_no, start=start_date, end=next_date, interval='1m')
        #/home/yoda/stock/price_data/KOSPI/minute/2023/0613/005930.csv
        file_path = "/home/yoda/stock/price_data/{}/{}/{}/{}/{}.csv".format(market_name, dataPeriod,now_year, to_date, info_num)
        directory = os.path.dirname(file_path)

        if not os.path.exists(directory):
            os.makedirs(directory)
        data.to_csv(file_path)

    done_file = "/home/yoda/stock/price_data/KOSDAQ/minute/2023/{}/DONE".format(to_date)
    open(done_file, "w").close()

@app.get("/stock-price/ticker={item_id}/day={exe_day}")
async def item_data(item_id: str, exe_day: str):
    # uri 받아오기
    name = item_id
    start_date = exe_day

    # 모듈에 필요한 날짜 계산하기
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    next_date = datetime.strftime(start_datetime + timedelta(days=1), "%Y-%m-%d")

    # 디렉토리만드는데 필요한 변수
    now_year = start_date.split('-')[0]
    info_num = name.split('.')[0]
    file_name = start_date[5:7] + start_date[8:10]

    data = yf.download(tickers=name, start=start_date, end=next_date, interval='1m')

    file_path = "/home/yoda/stock/price_data/{}/{}/{}_data.csv".format(now_year, info_num, file_name)
    directory = os.path.dirname(file_path)

    if not os.path.exists(directory):
        os.makedirs(directory)

    data.to_csv(file_path)

    return {"message": f"ticker number {item_id} saved successful"}



@app.get("/stock-price/kospi-day/day={exe_day}")
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
        dataPeriod ="day"

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

    done_file = "/home/yoda/stock/price_data/KOSPI/day/2023/{}/DONE".format(to_date)
    open(done_file, "w").close()
