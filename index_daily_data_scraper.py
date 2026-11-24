import pandas as pd
import yfinance as yf
from supabase import create_client
from datetime import datetime
import os
from dotenv import load_dotenv
import json

from idx_fetch import idx_get

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

proxy = os.environ.get("proxy")

# Fetch the URL
data = json.loads(idx_get(url, proxies={"http": proxy, "https": proxy} if proxy else None))
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

# Push to database. upsert, not insert: re-running a date already loaded must update
# instead of conflicting on every row (the old per-row insert swallowed the error).
# One statement for the whole date: a duplicate (index_code, date) pair would make Postgres
# reject the whole batch (ON CONFLICT cannot affect a row twice).
scrape_daily = scrape_daily.drop_duplicates(subset=['index_code', 'date'])
records = scrape_daily.to_dict(orient='records')
try:
    supabase.table("index_daily_data").upsert(records).execute()
except Exception as error:
    logging.error(f"Failed to upsert {len(records)} rows: {error}")
    raise
