from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContentSettings
import logging, os, sys
import pathlib as path
import inspect, json
import datetime, tempfile
try: import orbichem.src.pull_orbichem_data as pull_orbichem
except ModuleNotFoundError: import pull_acc_data as pull_acc

PROJECT_DIR = path.Path(__file__).parent.parent.parent

def get_and_config_logger(log_file):

    # log_dir = os.path.join(path.Path(__file__).parent.parent.resolve()
    # os.makedirs(log_dir, exist_ok=True)
    # logger = logging.getLogger(__name__)
    # logger.setLevel(logging.INFO)
    # handler = logging.StreamHandler(stream=sys.stdout)
    # formatter = logging.Formatter(fmt='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s')
    # handler.setFormatter(formatter)
    # logger.addHandler(handler)
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

def acc_download_http_response():
    
    func_name = inspect.currentframe().f_code.co_name
    logger, temp_file_name = get_and_config_logger(func_name)
    
    try:
        with open(os.path.join(PROJECT_DIR,"local.settings.json")) as f:
            data = json.load(f)
            adls_conn_string = data["Values"]["WEBSITE_CONTENTAZUREFILECONNECTIONSTRING"]
    except FileNotFoundError or FileNotFoundError or KeyError:
        adls_conn_string = os.environ["WEBSITE_CONTENTAZUREFILECONNECTIONSTRING"]

    try:
        orb = pull_acc.acc()
        orb.main_acc()
    
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
    acc_download_http_response()