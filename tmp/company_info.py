import FinanceDataReader as fdr

df_krx = fdr.StockListing("KRX")

# 종목번호 앞에 0으로 시작하는 애들을 살려주기 위해 str타입으로 변환
df_krx['Code']=df_krx['Code'].astype(str)
print(df_krx)

# tmp path to download csv
address = r"/Users/woorek/Downloads/"
df_krx.to_csv(path_or_buf=address+"company_info.csv")
