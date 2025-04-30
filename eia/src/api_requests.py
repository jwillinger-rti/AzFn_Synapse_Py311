import requests, json
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class RESTAPI():

    def __init__(self):
          pass
    
    def execute_calls_get_objects(self, endpoint_list:list, dataset:list):
        # Define the retry strategy.
        retry_strategy = Retry(
            total=4,  # Maximum number of retries.
            status_forcelist=[429, 500, 502, 503, 504],  # the HTTP status codes to retry on.
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        
        # Create a new session object.
        session = requests.Session()
        session.mount("https://", adapter)
        
        # ref op cap: 
        responses_dict = {}
        for n, data in enumerate(dataset):
            keys = [k for k in data.keys()]; key = keys[0]
            response = session.get(endpoint_list[n])
            status_code = response.status_code
            if status_code == 200:
                print(f"Success: {status_code}. {endpoint_list[n]}")
                responses_dict[data[key]]=response
            else:
                print(f"Failed: {status_code}. {endpoint_list[n]}")
                responses_dict[data[key]] = []

        return responses_dict