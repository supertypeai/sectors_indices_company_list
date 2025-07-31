from dotenv     import load_dotenv 
from datetime   import datetime

import os 
import logging 
import ssl
import urllib.request
import json 
import time 
import random 
import zipfile


# Setup Logging
logging.basicConfig(
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
        """
        Initializes the ProxyRequester class with the provided proxy

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
    
    def get_unzip_files(self, save_dir: str, file_name: str) -> list[str]:
        """ 
        Unzips a file and returns the list of files contained in the zip archive.

        Args:
            save_dir (str): Directory where the zip file is saved.
            file_name (str): Name of the zip file to be unzipped.

        Returns:
            list[str]: List of file names contained in the zip archive.
        """
        zip_path = os.path.join(save_dir, file_name)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            return zip_ref.namelist()


# Requester api url 
REQUESTER = ProxyRequester(proxy=PROXY)


def get_zip_files():
    """ 
    Efficiently downloads the minimum number of zip files needed to cover all specified indices.
    """
    indices_list = ["IDX30",'LQ45','KOMPAS100', "IDX BUMN20",'IDX HIDIV20', 
                    'IDX G30','IDX V30', "IDX Q30", "IDX ESGL", "SRIKEHATI", 
                    "SMINFRA18", "JII70", "ECONOMIC30", "IDXVESTA28"]
    
    remaining_indices = indices_list.copy()

    current_year = datetime.now().year
    base_url = "https://www.idx.co.id"
    save_dir = "source_data"
    os.makedirs(save_dir, exist_ok=True)

    # Loop as long as remaining_indices is not empty
    while remaining_indices:
        current_index = remaining_indices[0]

        if current_index == 'SRIKEHATI':
            encoded_indices = 'SRI-KEHATI'
        else:
            encoded_indices = current_index.replace(' ','')
        
        LOGGER.info(f"Fetching data for index: {current_index} for year {current_year}")

        # Construct the API URL for the current index
        api_url = f"https://www.idx.co.id/secondary/get/StockData/GetStockUploader?typeIndex={encoded_indices}&year={current_year}&table=stockIndex&locale=id"
        response = REQUESTER.fetch_url(api_url)
        if response == False:
            LOGGER.warning("Error accesing api url.") 
            remaining_indices.pop(0)
            continue 

        # Parse the JSON response
        datas = json.loads(response)
        
        # Get data result 
        data_results = datas.get('Results')
        if not data_results:
            LOGGER.info(f"No data found for index: {current_index}. Removing and continuing")
            remaining_indices.pop(0)
            continue

        # Extract latest file
        latest_result = data_results[0]
        original_file_name = latest_result.get('AttachmentName')
        if not original_file_name:
            remaining_indices.pop(0)
            continue

        # Proceed with download for new file
        url_to_download = latest_result.get('AttachmentUrl')
        full_url = base_url + url_to_download
        LOGGER.info(f"Downloading file: {full_url}")

        success = REQUESTER.download_file(full_url, os.path.join(save_dir, original_file_name))
        
        if success:
            indices_found_in_zip = set()
            extracted_files = REQUESTER.get_unzip_files(save_dir, original_file_name)

            # Check excel files that contains indices list
            for excel_file in extracted_files:
                for index_to_check in remaining_indices:
                    if index_to_check in excel_file:
                        indices_found_in_zip.add(index_to_check)
            
            LOGGER.info(f"Downloaded zip for '{current_index}'. It covers: {list(indices_found_in_zip)}")

            if not indices_found_in_zip:
                LOGGER.info(f'Move to the next remaining indices')
                indices_found_in_zip.add(current_index)

            # Drop any indices list already found
            remaining_indices = [indices for indices in remaining_indices if indices not in indices_found_in_zip]

        else:
            # If download failed, just remove the one we tried
            remaining_indices.pop(0)

        LOGGER.info(f"{len(remaining_indices)} indices remaining: {remaining_indices}")

        # Random sleep to avoid hitting the server too hard
        random_sleep = random.uniform(3, 12)
        time.sleep(random_sleep)
    
    LOGGER.info("All indices were successfully processed")


if __name__ == '__main__':
    get_zip_files()

