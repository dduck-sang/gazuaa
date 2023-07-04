import os
import mysql.connector
import pandas as pd

conn = mysql.connector.connect(user='stock', password='1234', host='192.168.90.128', database='stock', port='3306', auth_plugin='mysql_native_password')

final_ticker = []

cursor = conn.cursor()
query = 'select company_code from kospi_code'
cursor.execute(query)
result = cursor.fetchall()
ticker_list = [row[0] for row in result]
conn.close()

for ticker in ticker_list:
    final_ticker.append(ticker.strip().split('.')[0])

base_path = []

def print_file_paths(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            base_path.append(file_path)

directory = '/home/yoda/stock/price_data/KOSPI/minute/2023'
print_file_paths(directory)

tmp_base = []
for base in base_path:
    month = base.split('/')[8]

    if month.startswith("06"):
        tmp_base.append(base)

for ticker in final_ticker:
    final_path = []
    for base in tmp_base:
        code = base.split('/')[9].split('.')[0]
        if code == ticker:
            final_path.append(base)
    output_path = '/home/yoda/test/06/{}.csv'.format(ticker)
    merged_data = pd.DataFrame()
    for path in final_path:
        data = pd.read_csv(path)
        if 'Date' in data.columns:
            data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        else:
            data = data[['Datetime', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        merged_data = pd.concat([merged_data, data], ignore_index=True)
    merged_data.to_csv(output_path, index=False)
