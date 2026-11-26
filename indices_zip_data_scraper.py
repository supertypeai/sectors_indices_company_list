from dotenv import load_dotenv 
from datetime import datetime
from urllib.parse import quote

import os 
import logging 
import json 
import time 
import random 
import sys
import zipfile

from idx_announcement import index_field, normalise
from idx_fetch import idx_get


logging.basicConfig(
    level=logging.INFO, 
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
        self.proxies = {"http": proxy, "https": proxy} if proxy else None

    def fetch_url(self, url) -> str:
        """
        Fetches the content of a URL.
        
        Args:
            url (str): The URL to fetch.
        
        Returns:
            str: The content of the URL as a string, or False if an error occurs.
        """
        try:
            return idx_get(url, proxies=self.proxies).decode()
            
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
            file_content = idx_get(url, kind="zip", proxies=self.proxies)

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


def get_zip_files():
    """ 
    Efficiently downloads the minimum number of zip files needed to cover all specified indices.
    """
    requester = ProxyRequester(proxy=PROXY)

    indices_list = [
        "IDX30",'LQ45','KOMPAS100', "IDX BUMN20",'IDX HIDIV20', 
        'IDX G30','IDX V30', "IDX Q30", "IDX ESGL", "SRIKEHATI", 
        "SMINFRA18", "JII70", "ECONOMIC30", "IDXVESTA28"
    ]
    
    remaining_indices = indices_list.copy()

    content_mismatch_failures = []
    download_failures = []

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

        api_url = f"https://www.idx.co.id/secondary/get/StockData/GetStockUploader?typeIndex={encoded_indices}&year={current_year}&table=stockIndex&locale=id"
        response = requester.fetch_url(api_url)

        if response == False:
            # A blocked listing (403/502) used to be a silent skip: every index dropped, no zip
            # downloaded, exit 0. Count it so the run fails instead of looking healthy.
            LOGGER.error(f"Listing request failed for {current_index}: blocked or unreachable")
            download_failures.append(current_index)
            remaining_indices.pop(0)
            continue 

        datas = json.loads(response)
        
        data_results = datas.get('Results')

        if not data_results:
            LOGGER.error(f"No upload listed for index: {current_index}")
            download_failures.append(current_index)
            remaining_indices.pop(0)
            continue

        # Extract latest file
        latest_result = data_results[0]
        original_file_name = latest_result.get('AttachmentName')

        if not original_file_name:
            LOGGER.error(f"Listing for {current_index} has no attachment name")
            download_failures.append(current_index)
            remaining_indices.pop(0)
            continue

        # Proceed with download for new file
        url_to_download = latest_result.get('AttachmentUrl').replace('\\', '/')
        full_url = base_url + quote(url_to_download, safe='/:?&=')
        LOGGER.info(f"Downloading file: {full_url}")

        success = requester.download_file(full_url, os.path.join(save_dir, original_file_name))

        if not success:
            download_failures.append(current_index)
            remaining_indices.pop(0)
            continue 

        # Coverage is decided by the sheet's own index field, not a substring of the file name:
        # "IDX LQ45 Low Carbon Leader" contains "LQ45" but is a different index.
        covered = {
            index_field(sheet)
            for sheet in requester.get_unzip_files(save_dir, original_file_name)
        } - {None}
        indices_found_in_zip = [index for index in remaining_indices if normalise(index) in covered]
        
        LOGGER.info(f"Downloaded zip for '{current_index}'. It covers: {list(indices_found_in_zip)}")
        
        # If index excel did not found, add to indices_found_in_zip then can be drop
        if not indices_found_in_zip:
            LOGGER.info(f"Zip for {current_index} downloaded, but its contents did not match any remaining indices")
            content_mismatch_failures.append(current_index)
            remaining_indices.pop(0)

        # Drop any indices list already found
        remaining_indices = [index for index in remaining_indices if normalise(index) not in covered]

        LOGGER.info(f"{len(remaining_indices)} indices remaining: {remaining_indices}")

        random_sleep = random.uniform(3, 12)
        time.sleep(random_sleep)
    
    if download_failures:
        LOGGER.error(f"Failed to obtain index files for: {download_failures}")

    if not content_mismatch_failures and not download_failures:
        LOGGER.info("All indices were successfully processed")

    if content_mismatch_failures:
        LOGGER.error(f"Downloaded, but the contents matched no expected index: {content_mismatch_failures}")

    # Exit non-zero so the monthly workflow stops instead of letting data_automation.py
    # re-push the stale company_list CSVs (that is how it stayed green while broken).
    if download_failures or content_mismatch_failures:
        sys.exit(1)


if __name__ == '__main__':
    get_zip_files()

