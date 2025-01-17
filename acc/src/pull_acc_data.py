import os, datetime, pathlib as path
import tempfile, json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
import azure.identity
from azure.keyvault.secrets import SecretClient

PROJECT_DIR = path.Path(__file__).parent.parent.parent

class acc():

    def __init__(self):
        
        try:
            with open(os.path.join(PROJECT_DIR,"local.settings.json")) as f:
                data = json.load(f)
                kv_env = data["Values"]["KEYVAULT_ENV"]
                acc_login_url = data["Values"]["ACC_LOGIN_URL"]
                acc_main_url = data["Values"]["ACC_MAIN_URL"]
                acc_download_url = data["Values"]["ACC_DOWNLOAD_URL"]
                storage_account_key_for_synapse = data["Values"]["ADLS_STORAGEACCOUNTKEY_FORSYNAPSE"]
                storage_account_name_for_synapse = data["Values"]["ADLS_STORAGEACCOUNTNAME_FORSYNAPSE"]
                b_is_local = data["Values"]["IS_RUNNING_LOCALLY"]
        except FileNotFoundError or FileNotFoundError or KeyError:
            kv_env = os.environ["KEYVAULT_ENV"]
            acc_login_url = os.environ["ACC_LOGIN_URL"]
            acc_main_url = os.environ["ACC_MAIN_URL"]
            acc_download_url = os.environ["ACC_DOWNLOAD_URL"]
            storage_account_key_for_synapse = os.environ["ADLS_STORAGEACCOUNTKEY_FORSYNAPSE"]
            storage_account_name_for_synapse = os.environ["ADLS_STORAGEACCOUNTNAME_FORSYNAPSE"]
            b_is_local = os.environ["IS_RUNNING_LOCALLY"]

        if b_is_local == True:
            az_credential = azure.identity.AzureCliCredential()
        else: 
            az_credential = azure.identity.ManagedIdentityCredential()
        secret_client = SecretClient(vault_url=f"https://rti-rspaciq-kv{kv_env}.vault.azure.net",
                                        credential=az_credential)
        username = secret_client.get_secret("ACC-scrape-uid")
        password = secret_client.get_secret("ACC-scrape-pwd")

        # URLs and credentials
        self.login_url = acc_login_url
        self.main_url = acc_main_url
        self.download_url = acc_download_url
        self.keyvault_env = kv_env
        self.username = username
        self.password = password
        self.storage_account_name_for_synapse = storage_account_name_for_synapse
        self.storage_account_key_for_synapse = storage_account_key_for_synapse
        self.is_local = b_is_local

    def read_from_blob(self, where_from, what='json'):
        container_name = 'rti-synapse-db'
        blob_service_client = BlobServiceClient(
            account_url=f"https://{self.storage_account_name_for_synapse}.blob.core.windows.net",
            credential=self.storage_account_key_for_synapse)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(where_from)
        blob_data = blob_client.download_blob().readall()
        return json.loads(blob_data)

    def write_to_blob(self, where_to_write, what_to_write):
        container_name = 'rti-synapse-db'
        blob_service_client = BlobServiceClient(
            account_url=f"https://{self.storage_account_name_for_synapse}.blob.core.windows.net",
            credential=self.storage_account_key_for_synapse)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(where_to_write)
        blob_client.upload_blob(what_to_write, overwrite=True)

    def convert_period_date(self, period_date):
        # Extract the timestamp (milliseconds since epoch)
        timestamp = int(period_date[6:-2])

        # Convert timestamp to datetime object
        dt = datetime.datetime.utcfromtimestamp(timestamp / 1000.0)
        
        # Format the datetime object to ISO 8601 format (2024-06-01T09:00:00+05:00)
        # Assuming the desired timezone is UTC+5:00
        target_timezone = datetime.timezone(datetime.timedelta(hours=5))
        dt_with_tz = dt.replace(tzinfo=datetime.timezone.utc).astimezone(target_timezone)

        # Format the datetime to the desired string format
        formatted_date = dt_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")

        # Convert the timezone part to match the +hh:mm format
        formatted_date = formatted_date[:-2] + ":" + formatted_date[-2:]

        # URL encode the time portion
        url_encoded_date = formatted_date.replace(":", "%3A").replace("+", "%2B")

        return url_encoded_date

    def get_payloads4(self):
        reports = [
            'PE Inventory - US',
            'PE Capacity',
            'HDPE Preliminary production and sales',
            'HDPE Final production and end use sales',
            'LDPE Preliminary production and sales',
            'LDPE Final production and end use sales',
            'LLDPE Preliminary production and sales',
            'LLDPE Final production and end use sales',
            'Polypropylene Preliminary production and sales',
            'Polypropylene Final production and end use sales',
            'Polypropylene Inventory',
            'Polypropylene Capacity',
            'Polystyrene Preliminary production and sales',
            'Polystyrene Final production and end use sales',
            'Polystyrene Inventory',
            'Polystyrene Capacity',
            'PVC Preliminary production and sales',
            'PVC Final production and end use sales',
            'PVC Inventory',
            'PVC Capacity',
        ]
        
        blob_name = 'monthlies-web-data/json/data.json'
        data=self.read_from_blob(blob_name, 'json')
        
        # Filter data
        filtered_data = {"Data": [item for item in data['Data'] if item.get('FullProductName') in reports]}

        # Convert PeriodDate
        for item in filtered_data["Data"]:
            item["PeriodDate"] = self.convert_period_date(item["PeriodDate"])

        # Get queries
        blob_name = 'monthlies-web-data/json/queries.json'
        queries_list= self.read_from_blob(blob_name, 'json')

        # Update queries with formatted names
        for item in queries_list:
            name = item["Name"].replace("-", " ").replace(" US", " - US")
            # print(name)
            handle = name.split()
            handle = " ".join(handle[:2])
            # print(handle)
            
            for i in filtered_data["Data"]:
                if handle in i["FullProductName"]:
                    item["PeriodDate"] = i["PeriodDate"]
                    item["Name"] = f"{handle} {i['ReportingPeriod']} Industry Report"
                    mylist= item["Name"].split()
                    item["Name"] = '-'.join(mylist)
                    # print(item["Name"])
                    item["PeriodDate"] = i["PeriodDate"]
                    # print(item["PeriodDate"])
                    break

        queries_json = json.dumps(queries_list)

        # Write queries
        blob_name = 'monthlies-web-data/json/queries.json'
        self.write_to_blob(blob_name, queries_json)

        # Get old payloads
        blob_name = 'monthlies-web-data/json/payloads.json'
        payloads_list = self.read_from_blob(blob_name, 'json')
        print(f"{payloads_list}")

        # Update payloads with query strings
        for item in payloads_list:
            name = item["name"].replace("-", " ").replace(" US", "-US").replace("-US", " - US")
            handle = name.split()
            handle = "-".join(handle[:2])
            # print(handle)

            for i in queries_list:
                if handle in i["Name"]:
                    item["name"] = i["Name"]
                    item['queries'] = (
                        f"{i.get('Start', '')}CompanyId={i.get('CompanyId', '')}&"
                        f"FrequencyId={i.get('FrequencyId', '')}&"
                        f"Name={i.get('Name', '')}&"
                        f"PeriodDate={i.get('PeriodDate', '')}&"
                        f"ProductId={i.get('ProductId', '')}&"
                        f"ProductName={i.get('ProductName', '')}"
                    )
                    break
        payloads_json = json.dumps(payloads_list)
        self.write_to_blob(blob_name, payloads_json)
        print(payloads_json)
    
    def execute_acc(self):
        with requests.Session() as session:
            # Initial GET request to fetch the login page
            response = session.get(self.login_url)

        # Parse response for the CSRF token from the form
        soup = BeautifulSoup(response.content, 'html.parser')
        request_verification_token = soup.find('input', {'name': '__RequestVerificationToken'}).get('value')

        # Extract cookies
        header_request_token = response.cookies.get('__RequestVerificationToken')
        
        # Construct headers for the POST request to login
        login_headers = {
            "Cache-Control": "max-age=0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": f"__RequestVerificationToken={header_request_token}",
            "Origin": "https://pips.vaultconsulting.com",
            "Referer": self.login_url,
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        }
        
        # Prepare data payload for login
        data = {
            "__RequestVerificationToken": request_verification_token,
            "Item.Username": self.username.value,
            "Item.Password": self.password.value
        }

        # POST request to login
        session.post(self.login_url, headers=login_headers, data=data)

        # Extract the ASPXAUTH cookie after login
        ASPXAUTH = session.cookies.get('.ASPXAUTH')

        # Headers for subsequent requests
        headers = {
            "authority": "pips.vaultconsulting.com",
            "method": "GET",
            "path": "/reports",
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9,uz;q=0.8",
            "cookie": f"__RequestVerificationToken={header_request_token}; .ASPXAUTH={ASPXAUTH}; app.context-company=10121; app.filter-company=10121",
            "priority": "u=0, i",
            "referer": "https://pips.vaultconsulting.com/",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36"
        }

        # POST request to retrieve report data (data_json equivalent from previous step)
        response = session.post("https://pips.vaultconsulting.com/reports/myreports_read", headers=headers)
        text = response.text
        
        blob_name = 'monthlies-web-data/json/data.json'
        self.write_to_blob(blob_name, text)
        self.get_payloads4() # Execute, is there any purpose to this?
        
        # After login, send POST request to download URL
        download_headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9,ru;q=0.8",
            "content-type": "application/json",
            "origin": "https://pips.vaultconsulting.com",
            "referer": "https://pips.vaultconsulting.com/",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }

        cookies = {
            '__RequestVerificationToken': header_request_token,
        }

        # %run Scrape Monthlies Data/read_from_blob
        blob_name = 'monthlies-web-data/json/payloads.json'
        payloads = self.read_from_blob(blob_name,'json')

        # Azure Blob Storage credentials
        container_name = 'rti-synapse-db'
        directory_name = 'ACC'
        account_url = f"https://{self.storage_account_name_for_synapse}.blob.core.windows.net/"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=self.storage_account_key_for_synapse)
        container_client = blob_service_client.get_container_client(container_name)

        for item in payloads:
            
            # Send POST request to download the report
            download_response = session.post(self.download_url, headers=download_headers, cookies=cookies, json=item)
            
            # Parse the JSON response to extract the actual download URL
            response_data = download_response.json()
            file_download_url = response_data.get("Data")

            # Complete the base URL for the download
            full_download_url = f"https://pips.vaultconsulting.com{file_download_url}"
            
            # download the file from the extracted URL
            file_response = session.get(full_download_url, cookies=cookies)

            # Define the blob name including the directory
            excel_blob_name = f"{directory_name}/output.xlsx"
            blob_client = container_client.get_blob_client(excel_blob_name)
            
            # Assuming `file_response.content` contains the bytes of the Excel file
            blob_client.upload_blob(file_response.content, overwrite=True)

            # Download the Excel file from Blob Storage
            with tempfile.NamedTemporaryFile(prefix="acc_scrape_output", suffix=".xlsx", mode='a', delete=False) as temp_file:
                temp_file_name = temp_file.name
            with open(temp_file_name , 'wb') as file:
                download_stream = blob_client.download_blob()
                file.write(download_stream.readall())

            # Read the Excel file into a DataFrame
            df = pd.read_excel(temp_file_name)
            # print(df)
            # Construct the CSV file name based on item['name']
            csv_filename = f"{item['name']}"
            with tempfile.NamedTemporaryFile(prefix=csv_filename, suffix=".csv", mode='a', delete=False) as temp_file:
                tempcsv_file_name = temp_file.name

            # Save the DataFrame as a CSV file locally
            df.to_csv(tempcsv_file_name, index=False)

            # Upload the CSV file to the specified directory in Blob Storage
            csv_blob_name = f"{directory_name}/{csv_filename}"
            csv_blob_client = container_client.get_blob_client(csv_blob_name)

            with open(tempcsv_file_name, "rb") as data:
                csv_blob_client.upload_blob(data, overwrite=True)
    
    def main_acc(self): 
        self.execute_acc()


if __name__ == "__main__":

    acc().main_acc()