import pandas as pd
import zipfile
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# Fetch company name data from idx_company_profile data in db
url_supabase = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, key)

idx_data = supabase.table("idx_company_profile").select("symbol","company_name").execute()
idx_data = pd.DataFrame(idx_data.data)

def delete_all_files(directory_path):
    # List all files in the directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        try:
            # Check if it is a file and delete it
            if os.path.isfile(file_path):
                os.remove(file_path)
            # Optionally, delete subdirectories and their contents
            elif os.path.isdir(file_path):
                os.rmdir(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

    return(f"All files in {directory_path} have been deleted.")


def unzip_file(file_directory, extract_directory):
    # Get list of files in the specified directory
    files = os.listdir(file_directory)

    # Filter only zip files
    zip_files = [f for f in files if os.path.isfile(os.path.join(file_directory, f)) and f.endswith('.zip')]

    for i in zip_files:
        with zipfile.ZipFile(f"source_data/{i}","r") as zip_ref:
            zip_ref.extractall(extract_directory)
    
    return print(f"Finish Unzip file from {file_directory} folder into {extract_directory} folder")

def company_list_extraction(data_path,idx_data):

    df = pd.read_excel(data_path, skiprows=7).iloc[1:,2:]

    # Convert the column to numeric, setting errors='coerce' will convert non-numeric values to NaN
    df["Rasio Free Float"] = pd.to_numeric(df["Rasio Free Float"], errors='coerce')

    # Drop rows with NaN values in the specified column
    df.dropna(subset=["Rasio Free Float"], inplace= True)

    df.rename(columns={"Kode":'symbol'}, inplace=True)

    df["symbol"] = df['symbol'].apply(lambda x: x + ".JK")

    df = df[["symbol"]]

    df = df.merge(idx_data,on="symbol")

    return df

# Unzip file
unzip_file("source_data", "source_data/extracted_data")

# Extract Symbol
files = os.listdir("source_data/extracted_data")

indices_list = ["IDX30",'LQ45','KOMPAS100', "IDX BUMN20",'IDX HIDIV20', 'IDX G30','IDX V30', "IDX Q30", "IDX ESGL", "SRIKEHATI", "SMINFRA18", "JII70", "ECONOMIC30"]

# Filter only zip files
excel_files = [f for f in files if os.path.isfile(os.path.join("source_data/extracted_data", f)) and f.endswith('.xlsx')]

for i in indices_list:
    try:
        file = [s for s in excel_files if i in s.upper()][0]

        data = company_list_extraction(f"source_data/extracted_data/{file}",idx_data)

        data.to_csv(f"company_list/companies_list_{i.lower().replace(' ','')}.csv", index=False)

        print(f"Company list for {i} index already extracted")
    except:
       print(f"No new update for {i} indices")

print("----------------------------------------------------------")

delete_all_files("source_data/extracted_data")