import yfinance as yf
import os
from datetime import datetime

import mysql.connector


def connect_to_mysql():
    conn = mysql.connector.connect(user='stock', password='1234', host='192.168.90.128', database='stock', port='3306',
                                   auth_plugin='mysql_native_password')
    return conn


def get_ticker_list():
    conn = connect_to_mysql()
    cursor = conn.cursor()
    query = 'select company_code from kospi_code'
    cursor.execute(query)
    result = cursor.fetchall()
    ticker_list = [row[0] for row in result]
    cursor.close()
    conn.close()
    return ticker_list


def create_directory(year, quarter):
    directory = '/Users/jesse/Documents/da/{}/{}/'.format(year, quarter)
    os.makedirs(directory, exist_ok=True)
    return directory


def download_data(ticker, start_date, end_date, directory):
    file_name = ticker.split('.')[0]
    data = yf.download(tickers=ticker, start=start_date, end=end_date, interval='1d')
    data.to_csv(os.path.join(directory, '{}.csv'.format(file_name)))


def main():
    ticker_list = get_ticker_list()
    year_list = ['2023']

    for year in year_list:
        for quarter in range(1, 5):
            if quarter == 1:
                start_date = datetime.strptime('{}-01-01'.format(year), '%Y-%m-%d')
                end_date = datetime.strptime('{}-04-01'.format(year), '%Y-%m-%d')
            elif quarter == 2:
                start_date = datetime.strptime('{}-04-01'.format(year), '%Y-%m-%d')
                end_date = datetime.strptime('{}-07-01'.format(year), '%Y-%m-%d')
            elif quarter == 3:
                start_date = datetime.strptime('{}-07-01'.format(year), '%Y-%m-%d')
                end_date = datetime.strptime('{}-10-01'.format(year), '%Y-%m-%d')
            else:
                start_date = datetime.strptime('{}-10-01'.format(year), '%Y-%m-%d')
                end_date = datetime.strptime('{}-12-31'.format(year), '%Y-%m-%d')

            directory = create_directory(year, quarter)

            for ticker in ticker_list:
                download_data(ticker, start_date, end_date, directory)


if __name__ == '__main__':
    main()
