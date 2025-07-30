from supabase   import create_client
from dotenv     import load_dotenv

import pandas as pd
import zipfile
import os


load_dotenv()

# Fetch company name data from idx_company_profile data in db
URL_SUPABASE = os.environ.get("SUPABASE_URL")
KEY_SUPABASE = os.environ.get("SUPABASE_KEY")
SUPABASE_CLIENT = create_client(URL_SUPABASE, KEY_SUPABASE)

IDX_DATA = SUPABASE_CLIENT.table("idx_company_profile").select("symbol","company_name").execute()
IDX_DATA = pd.DataFrame(IDX_DATA.data)


def delete_all_files(directory_path: str) -> str:
    """ 
    Deletes all files in the specified directory.

    Args:
        directory_path (str): Path to the directory from which files will be deleted.
    
    Returns:
        str: Confirmation message indicating that all files have been deleted.
    """
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


def unzip_file(file_directory: str, extract_directory: str):
    """ 
    Unzips all zip files in the specified directory to the extract directory.

    Args:
        file_directory (str): Directory containing the zip files.
        extract_directory (str): Directory where the files will be extracted.
    """
    # Get list of files in the specified directory
    files = os.listdir(file_directory)

    # Filter only zip files
    zip_files = [f for f in files if os.path.isfile(os.path.join(file_directory, f)) and f.endswith('.zip')]
    print(f"Check data zip files: {zip_files[:2]}")

    for zip_file in zip_files:
        zip_path = os.path.join(file_directory, zip_file)
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_directory)
        except Exception as error:
            print(f"Failed to unzip file: {zip_path}. Reason: {error}")
    
    return print(f"Finish Unzip file from {file_directory} folder into {extract_directory} folder")


def company_list_extraction(data_path: str, idx_data: pd.DataFrame) -> pd.DataFrame:
    """ 
    Extracts company list from the provided Excel file and merges it with idx_data.

    Args:
        data_path (str): Path to the Excel file containing company data.
        idx_data (pd.DataFrame): DataFrame containing idx company profile data.
    
    Returns:
        pd.DataFrame: DataFrame containing the extracted company symbols.
    """
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


def run_indices_update():
    """ 
    Main function to run the indices update process.
    Unzips the downloaded files, extracts company lists, and saves them.
    """
    # Unzip file
    unzip_file("source_data", "source_data/extracted_data")

    # Extract Symbol
    files = os.listdir("source_data/extracted_data")

    indices_list = ["IDX30",'LQ45','KOMPAS100', "IDX BUMN20",'IDX HIDIV20', 'IDX G30','IDX V30', "IDX Q30", "IDX ESGL", "SRIKEHATI", "SMINFRA18", "JII70", "ECONOMIC30"]

    # Filter only zip files
    excel_files = [f for f in files if os.path.isfile(os.path.join("source_data/extracted_data", f)) and f.endswith('.xlsx')]
    print(f"Check data excel files: {excel_files[:2]}")

    # Make sure company list dir
    os.makedirs("company_list", exist_ok=True)

    for indices in indices_list:
        try:
            file = [s for s in excel_files if indices in s.upper()][0]
            print(f"Check data file: {file[:2]}")
            
            data = company_list_extraction(f"source_data/extracted_data/{file}", IDX_DATA)
            print(f"Check data company list extracted: {data[:2]}")

            data.to_csv(f"company_list/companies_list_{indices.lower().replace(' ','')}.csv", index=False)

            print(f"Company list for {indices} index already extracted")
        except:
            print(f"No new update for {indices} indices")

    print("----------------------------------------------------------")


if __name__ == '__main__':
    run_indices_update()
    delete_all_files("source_data/extracted_data")
    delete_all_files("source_data")