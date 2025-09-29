import pandas as pd
import yfinance as yf
from supabase import create_client
from datetime import datetime
import ssl
import urllib.request
import os
from dotenv import load_dotenv
import json

load_dotenv()

import logging
from importlib import reload
logging.basicConfig(level=logging.ERROR)

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program started')

LOG_FILENAME = 'scrapper.log'
initiate_logging(LOG_FILENAME)

load_dotenv()

# Configure supabase client
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

# Fetch STI, KLSE, FTSE from yf
indices = ["^STI","^KLSE","WIIDN.FGI"]#["^JKLQ45","IDX30.JK","IDXHIDIV20.JK",'IDXBUMN20.JK',"IDXV30.JK","IDXG30.JK",'IDXQ30.JK','IDXESGL.JK',"SRI-KEHATI.JK","SMINFRA18.JK",'JII70.JK',"KOMPAS100.JK","^JKSE","ECONOMIC30.JK","IDXVESTA28.JK"]

scrape_daily = pd.DataFrame()

for i in indices:
    data = yf.download(i, period="1d", auto_adjust=False).reset_index()[["Date","Close"]]

    data.columns = ["date",'price']

    data['index_code_yf'] = i

    scrape_daily = pd.concat([scrape_daily,data])

scrape_daily["price"] = round(scrape_daily["price"],2)

index_df = pd.read_csv("index_name.csv")

scrape_daily = scrape_daily.merge(index_df, on="index_code_yf").drop('index_code_yf',axis=1)

scrape_daily["date"] = scrape_daily["date"].astype('str')

# Fetch Indonesian index from idx
date = datetime.strptime(scrape_daily["date"].unique()[0], "%Y-%m-%d").strftime("%Y%m%d")

# Replace this with the URL you want to fetch
url = f"https://www.idx.co.id/primary/TradingSummary/GetIndexSummary?date={date}&length=9999&start=0"

# Allow unverified SSL
ssl._create_default_https_context = ssl._create_unverified_context

# Set up proxy
proxy = os.environ.get("proxy")
proxy_support = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)

# Fetch the URL
try:
    with urllib.request.urlopen(url) as response:
        content = response.read().decode()
except Exception as e:
    print(f"Error fetching URL: {e}")

data = json.loads(content)
df = pd.DataFrame(data['data'])

idx_name = pd.read_csv("index_name.csv")

df = df[(df['IndexCode'].isin(idx_name['index_code']))|(df['IndexCode'] == "COMPOSITE")].reset_index()

df = df[['IndexCode', 'Date','Close']]

df.columns = ["index_code", 'date',"price"]

df["index_code"] = df["index_code"].apply(lambda x: "IHSG" if x == "COMPOSITE" else x) 

df['date'] = pd.to_datetime(df['date'])

df = df.merge(idx_name[["index_code",'index_name']], on="index_code")

scrape_daily = pd.concat([scrape_daily,df])

scrape_daily['date'] = pd.to_datetime(scrape_daily['date'])

scrape_daily["date"] = scrape_daily["date"].astype('str')

# Push to database
for sub_sector in range(0,scrape_daily.shape[0]):
    try:
        supabase.table("index_daily_data").insert(dict(scrape_daily.iloc[sub_sector])).execute()
    except:
        logging.error(f"Failed to update description for {sub_sector}.")