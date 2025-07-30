from supabase   import create_client
from dotenv     import load_dotenv

import pandas as pd
import os


load_dotenv()

# Fetch company name data from idx_company_profile data in db
URL_SUPABASE = os.environ.get("SUPABASE_URL")
KEY_SUPABASE = os.environ.get("SUPABASE_KEY")
SUPABASE_CLIENT = create_client(URL_SUPABASE, KEY_SUPABASE)


def get_all_indices() :
    """ 
    Fetches all indices data from the 'company_list' directory, combines them into a single DataFrame,
    and groups the data by 'symbol' to create a mapping of symbols to their associated indices.
    """
    company_list_dir = "company_list"
    all_indices_data = []

    # Check if the directory exists and has files
    if os.path.exists(company_list_dir) and os.listdir(company_list_dir):
        for filename in os.listdir(company_list_dir):
            if filename.endswith(".csv"):
                # Extract the index name from the filename
                index_name = filename.split('_')[-1].split('.')[0].upper()
                
                file_path = os.path.join(company_list_dir, filename)
                df = pd.read_csv(file_path)
                
                # Add a new column for the index name
                df['index'] = index_name
                all_indices_data.append(df)
    else:
        print("No CSV files found in 'company_list' directory. Exiting.")
        exit()

    combined_df = pd.concat(all_indices_data, ignore_index=True)
    print(f'length before remove duplicate check: {len(combined_df)}')
    combined_df.drop_duplicates(subset=['symbol', 'index'], inplace=True)
    print(f'length after remove duplicate check: {len(combined_df)}')

    grouped_indices = combined_df.groupby('symbol')['index'].apply(list)
    
    return grouped_indices.to_dict()


def push_to_supabase(grouped_indices: dict):
    """ 
    Pushes the grouped indices data to the Supabase database.

    Args:
        grouped_indices (pd.DataFrame): DataFrame containing symbols and their associated indices.
    """
    for symbol, indices in grouped_indices.items():
        response = (
            SUPABASE_CLIENT
                .table('idx_company_profile')
                .update({'indices': indices})
                .eq('symbol', symbol)
                .execute()
        )

        count = response.count or len(response.data or [])
        print(f"Updated {symbol}: {count} row(s)")


if __name__ == '__main__':
    grouped_indices = get_all_indices()
    push_to_supabase(grouped_indices)