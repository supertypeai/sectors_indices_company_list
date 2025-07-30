from dotenv     import load_dotenv 
from datetime   import datetime

import os 
import logging 
import ssl
import urllib.request
import json 
import time 
import random 


# Setup Logging
logging.basicConfig(
    # filename='indices_scraping.log', # Set a file for save logger output 
    level=logging.INFO, # Set the logging level
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )
LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")


# Setup .env
load_dotenv(override=True)
PROXY = os.getenv('PROXY')


class ProxyRequester:
    def __init__(self, proxy=None):
        """Initializes the ProxyRequester class with the provided proxy

        Args:
            proxy (str, optional): the proxy to be used. Defaults to None. Example: 'brd-customer-xxx-zone-xxx:xxx@brd.superproxy.io:xxx'
        """

        # Set up SSL context to unverified
        ssl._create_default_https_context = ssl._create_unverified_context

        proxy_support = urllib.request.ProxyHandler({'http': proxy,'https': proxy})
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)

    def fetch_url(self, url) -> str:
        """
        Fetches the content of a URL using the installed opener.
        
        Args:
            url (str): The URL to fetch.
        
        Returns:
            str: The content of the URL as a string, or False if an error occurs.
        """
        # Use the installed opener to fetch the URL
        try:
            with urllib.request.urlopen(url) as response:
                return response.read().decode()
        except Exception as error:
            print(f"Error fetching URL: {error}")
            return False
    
    def download_file(self, url: str, saved_dir: str) -> bool:
        """
        Downloads a file from the specified URL and saves it to the given directory.
        
        Args:
            url (str): The URL of the file to download.
            saved_dir (str): The directory where the file will be saved.
        
        Returns:
            bool: True if the file was downloaded and saved successfully, False otherwise.
        """
        try: 
            with urllib.request.urlopen(url) as response:
                file_content = response.read()

            with open(saved_dir, 'wb') as file_zip:
                file_zip.write(file_content)
            
            print(f"Zip files saved to: {saved_dir}")
            return True
        except Exception as error:
            print(f"Failed to download {url}. Error: {error}")
            return False


# Requester api url 
REQUESTER = ProxyRequester(proxy=PROXY)


def get_zip_files():
    """ 
    Downloads zip files from the IDX website for various indices and saves them to a local directory.
    """
    indices_list = ["IDX30",'LQ45','KOMPAS100', "IDX BUMN20",'IDX HIDIV20', 'IDX G30','IDX V30', "IDX Q30", "IDX ESGL", "SRIKEHATI", "SMINFRA18", "JII70", "ECONOMIC30"]
    current_year = datetime.now().year
    base_url = "https://www.idx.co.id"

    save_dir = "source_data"
    os.makedirs(save_dir, exist_ok=True)

    # Loop through each index and fetch the corresponding data
    for indices in indices_list:
        if indices == 'SRIKEHATI':
            encoded_indices = 'SRI-KEHATI'
        else:
            encoded_indices = indices.replace(' ','')
        
        LOGGER.info(f"Fetching data for index: {indices} for year {current_year}")

        # Construct the API URL for the current index
        api_url = f"https://www.idx.co.id/secondary/get/StockData/GetStockUploader?typeIndex={encoded_indices}&year={current_year}&table=stockIndex&locale=id"
        response = REQUESTER.fetch_url(api_url)
        if response == False:
             raise Exception("Error accesing api url.") 

        # Parse the JSON response
        datas = json.loads(response)
        
        # Get data result 
        data_results = datas.get('Results')
        if not data_results:
            LOGGER.info(f"No data found for index: {indices}. Skipping.")
            continue

        # Extract latest file
        latest_result = data_results[0]
       
        # Define the filename and full local path for saving
        original_file_name = latest_result.get('AttachmentName')
        _, extension = os.path.splitext(original_file_name) 
        new_file_name = f"index_data_idx_downloaded_{indices}{extension}"
        local_file_path = os.path.join(save_dir, new_file_name)

        # Proceed with download for new file
        url_to_download = latest_result.get('AttachmentUrl')
        full_url = base_url + url_to_download
        LOGGER.info(f"Downloading file: {full_url}")

        success = REQUESTER.download_file(full_url, local_file_path)
        
        if success:
            LOGGER.info(f"Successfully saved to {local_file_path}")
            # Random sleep to avoid hitting the server too hard
            random_sleep = random.uniform(3, 12)
            time.sleep(random_sleep)


if __name__ == '__main__':
    get_zip_files()

