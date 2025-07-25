####################################
# Author: Jon Willinger
# Date: 2025-02-14
# Notes: 
####################################

import os, csv, re
import logging
import pathlib as path
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import datetime, tempfile
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa
import pandas as pd, numpy as np
import inspect, json
from azure.storage.blob import BlobServiceClient, ContentSettings, BlobClient, BlobType
import azure.identity
try: import drivers.src.upload_blob as upb
except ModuleNotFoundError: import upload_blob as upb

PROJECT_DIR = path.Path(__file__).parent.parent.parent


def get_and_config_logger(log_file):

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    with tempfile.NamedTemporaryFile(mode='a', delete=False) as temp_file:
        temp_file_name = temp_file.name
    file_handler = logging.FileHandler(temp_file_name)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger, temp_file_name

def upload_log_to_blob(logger, temp_file_name, adls_conn_string):
    # TODO: replace with ENV
    container_name = "synapse-fn-logs"
    blob_name = f"{logger.name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"    
    adls_svc_client = BlobServiceClient.from_connection_string(adls_conn_string)
    container_client = adls_svc_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    blob_exists = blob_client.exists()
    
    if not blob_exists:
        with open(temp_file_name, "rb") as temp_file:
            container_client.upload_blob(name=blob_name, data=temp_file, content_settings=ContentSettings(content_type='text/plain'))
        print(f"Blob '{blob_name}' created with content from the temporary file.")
    else:
        existing_content = blob_client.download_blob().readall().decode('utf-8')
        with open(temp_file_name, "r") as temp_file:
            new_content = existing_content + temp_file.read()
        blob_client.upload_blob(data=new_content, overwrite=True)

def driverspdf_upload_http_response():
    func_name = inspect.currentframe().f_code.co_name
    logger, temp_file_name = get_and_config_logger(func_name)
    
    try:
        with open(os.path.join(PROJECT_DIR,"local.settings.json")) as f:
            data = json.load(f)
            adls_conn_string = data["Values"]["WEBSITE_CONTENTAZUREFILECONNECTIONSTRING"]
    
    except FileNotFoundError or KeyError:
        adls_conn_string = os.environ["WEBSITE_CONTENTAZUREFILECONNECTIONSTRING"]
    
    try:
    # host1 = "rti-synapse-db.sql.azuresynapse.net" # SBX
    # host2 = "rti-synapse-pd.sql.azuresynapse.net" # PRD
        driver = upb.driver_pdfs()
        driver.main()
    
    except Exception as e:
        logger.error(e)
        logger.error("run failed. \n")
        b_success = False
    else:
        logger.info("run successful. \n")
        b_success = True
    
    upload_log_to_blob(logger, temp_file_name, adls_conn_string)
    
    return b_success


if __name__ == "__main__":

    driverspdf_upload_http_response()