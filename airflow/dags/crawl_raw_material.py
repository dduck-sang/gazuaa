from bs4 import BeautifulSoup as bs
import requests, re, datetime


def get_material_id(base_url):
    response = requests.get(url=base_url,headers=headers)
    soup = bs(response.text, 'html.parser')
    tr_tag = soup.findAll('tr', id=re.compile(r"pair_\d+"))
    tr_id = []
    for i in tr_tag:
        tr_id.append(i['id'].split('_')[1])
    return tr_id

# get_url = r"https://api.investing.com/api/financialdata/historical/{}?start-date={}&end-date={}&time-frame=Daily&add-missing-rows=false"
def get_material_data(pair_id, data_url, start_date, end_date):
    formatted_url = data_url.format(pair_id)
    params = {
        "start-date":start_date,
        "end-date":end_date,
        "time-frame": "Daily",
        "add-missing-rows":"false"
    }
    response = requests.get(url=formatted_url, params=params, headers=headers)
    resp_data = response.json()
    data = resp_data["data"]
    return data


headers = {
    "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Domain-Id" : "www"
}

base_url = r"https://www.investing.com/commodities/"
data_url = r"https://api.investing.com/api/financialdata/historical/{}"
start_date = '2023-01-01'
end_date = datetime.datetime.now().strftime("%Y-%m-%d")

id_list = get_material_id(base_url=base_url)
data = {}
for i in id_list:
    pair_id = i
    material_data = get_material_data(pair_id=pair_id, data_url=data_url, start_date=start_date, end_date=end_date)
    data[i]=material_data
print(data)
 
