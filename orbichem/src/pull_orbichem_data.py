# Capro:
# https://orbichem360.orbichem.com/price/monitor/
# Tecnon Orbichem

import os, json, pathlib as path
import requests
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ContentSettings
import azure.identity
from azure.keyvault.secrets import SecretClient

PROJECT_DIR = path.Path(__file__).parent.parent.parent

class orbichem_capro():
    
    def __init__(self, host):
        
        try:
            with open(os.path.join(PROJECT_DIR,"local.settings.json")) as f:
                data = json.load(f)
                kv_env = data["Values"]["KEYVAULT_ENV"]
                storage_account_key_for_synapse = data["Values"]["ADLS_STORAGEACCOUNTKEY_FORSYNAPSE"]
                storage_account_name_for_synapse = data["Values"]["ADLS_STORAGEACCOUNTNAME_FORSYNAPSE"]
                b_is_local = data["Values"]["IS_RUNNING_LOCALLY"]
        except FileNotFoundError or KeyError:
            kv_env = os.environ["KEYVAULT_ENV"]
            storage_account_key_for_synapse = os.environ["ADLS_STORAGEACCOUNTKEY_FORSYNAPSE"]
            storage_account_name_for_synapse = os.environ["ADLS_STORAGEACCOUNTNAME_FORSYNAPSE"]
            b_is_local = os.environ["IS_RUNNING_LOCALLY"]
        
        if b_is_local == True:
            az_credential = azure.identity.AzureCliCredential()
        else: 
            az_credential = azure.identity.DefaultAzureCredential()
        secret_client = SecretClient(vault_url=f"https://rti-rspaciq-kv{kv_env}.vault.azure.net",
                                        credential=az_credential)
        orbichem_uid = secret_client.get_secret("ORBICHEM-UID")
        orbichem_pw = secret_client.get_secret("ORBICHEM-PW")

        # URLs
        self.capro_login_url = "https://orbichem360.orbichem.com/auth/signin?from_page=/"
        self.capro_url = "https://orbichem360.orbichem.com/price/monitor/get_price_data"
        self.host = host
        self.orbichem_uid = orbichem_uid
        self.orbichem_pw = orbichem_pw
        self.storage_account_key_for_synapse = storage_account_key_for_synapse
        self.storage_account_name_for_synapse = storage_account_name_for_synapse

    def upload_dataframe_to_azure_blob(self, dataframe, directory, file_name):

        csv_data = dataframe.to_csv(index=False)
        container_name = "rti-synapse-db"
        blob_name = f"{directory}/{file_name}"

        blob_service_client = BlobServiceClient(account_url=f"https://{self.storage_account_name_for_synapse}.blob.core.windows.net",
                                                credential=self.storage_account_key_for_synapse)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(csv_data, blob_type="BlockBlob", overwrite=True, content_settings=ContentSettings(content_type='text/csv'))

    def main_capro(self):
        
        print("Executing")
        username = self.orbichem_uid.value
        password = self.orbichem_pw.value
        # Current date and time
        current_date = datetime.now()

        # First day of the current month
        first_day_of_current_month = current_date.replace(day=1).strftime('%m/%d/%Y')

        # First day of the previous month
        first_day_previous_month = (current_date.replace(day=1) - timedelta(days=1)).replace(day=1)
        first_day_previous_month_formatted = first_day_previous_month.strftime('%Y-%m-%d')

        with requests.Session() as session:
            # Login to the website
            login_payload = {
                'username': username,
                'password': password
            }
            response = session.post(self.capro_login_url, data=login_payload)

            # Define request headers and data
            headers = {
                "accept": "application/json, text/javascript, */*; q=0.01",
                "accept-language": "en-US,en;q=0.9,uz;q=0.8",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Google Chrome\";v=\"126\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "x-requested-with": "XMLHttpRequest"
            }

            data = {
                "start_date": "01/01/2014",
                "end_date": f"{first_day_of_current_month}",
                "currency": "USD",
                "cbf_group": "9",
                "chemical[]": "20",
                "uom_id": "0",
                "regions[]": ["9"],  # 9 - China
                "definitions[]": ["1129"], # Domestic Spot
                "panel_states[dv_cont_1][data]": "",
                "panel_states[dv_cont_1][time_scale]": "M",
                "panel_states[dv_cont_4][data]": "",
                "panel_states[dv_cont_4][time_scale]": "",
                "product_id": "20" # Caprolactam
            }

            # Send POST request to fetch data
            response = session.post(self.capro_url, headers=headers, data=data)
            response.raise_for_status()  # Raise an error for bad response status codes
            
            # Parse JSON response
            json_data = response.json()

            # Find matching entry for the first day of the previous month:
            matching_entries = [entry for entry in json_data['price_data'] if entry['date'] == first_day_previous_month_formatted]

            # Create DataFrame from matching entry
            if matching_entries:
                capro_df = pd.DataFrame(matching_entries[0], index=[0])
                capro_df = capro_df[['date', 'name', 'region', 'definition', 'primary_low', 'primary_high', 'converted_low', 'converted_high', 'price']]
                capro_df = capro_df.rename(columns={'date': 'price_date'})
                # capro_df['load_date'] = today

        directory = "drivers-web-data/capro"
        file_name = f"capro_{first_day_previous_month.strftime('%Y%m%d')}.csv"
        self.upload_dataframe_to_azure_blob(dataframe=capro_df,
                directory=directory, file_name=file_name)
        

if __name__ == "__main__":

    with open(os.path.join(PROJECT_DIR, "local.settings.json")) as f:
            data = json.load(f)
            host = data["Values"]["SYNAPSE_INSTANCE"]

    orb = orbichem_capro(host)
    orb.main_capro()