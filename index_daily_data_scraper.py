import pandas as pd
import yfinance as yf
from supabase import create_client

import os
from dotenv import load_dotenv

load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR)

load_dotenv()

# Configure supabase client
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

indices = ["^JKLQ45","IDX30.JK","IDXHIDIV20.JK",'IDXBUMN20.JK',"WIIDN.FGI","IDXV30.JK","IDXG30.JK",'IDXQ30.JK','IDXESGL.JK',"SRI-KEHATI.JK","SMINFRA18.JK",'JII70.JK',"KOMPAS100.JK","^JKSE","^STI","^KLSE","ECONOMIC30.JK"]

scrape_daily = pd.DataFrame()

for i in indices:
    data = yf.download(i, period="1d").reset_index()[["Date","Close"]]

    data.columns = ["date",'price']

    data['index_code_yf'] = i

    scrape_daily = pd.concat([scrape_daily,data])

scrape_daily["price"] = round(scrape_daily["price"],2)

index_df = pd.read_csv("index_name.csv")

scrape_daily = scrape_daily.merge(index_df, on="index_code_yf").drop('index_code_yf',axis=1)

scrape_daily["date"] = scrape_daily["date"].astype('str')

for sub_sector in range(0,scrape_daily.shape[0]):
    try:
        supabase.table("index_daily_data").insert(dict(scrape_daily.iloc[sub_sector])).execute()
    except:
        logging.error(f"Failed to update description for {sub_sector}.")