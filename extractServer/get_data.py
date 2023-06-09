from fastapi import FastAPI, Path
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import os

app = FastAPI()

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

