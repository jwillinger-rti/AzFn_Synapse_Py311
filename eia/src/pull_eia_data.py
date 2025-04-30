####################################
# Author: Jon Willinger
# Date: 2024-12-09
# Notes: 
# api key registration url: https://www.eia.gov/opendata/register.php (api license terms here).
# api key name: Jon Willinger (Can be anyone; email is available to the team).
# api key registration email: resinsmartauto@rtiglobal.com
# eia front url: https://www.eia.gov/dnav/pet/PET_SUM_SNDW_A_(NA)_YUP_PCT_W.htm
#
# Notes: Percent Utilization is calculated as gross inputs divided by the latest 
# reported monthly operable capacity (using unrounded numbers).  See Definitions, 
# Sources, and Notes link above for more information on this table.
#
####################################

# Expand API Calls:

import os
import requests, json
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import logging, pathlib as path
import datetime
import pandas as pd
import sqlalchemy as sa
import azure.identity
from azure.keyvault.secrets import SecretClient
from azure.core import exceptions
from typing import List
from sqlalchemy.orm.session import sessionmaker
try: import eia.src.azsynapse as azsyn
except ModuleNotFoundError: import azsynapse as azsyn
try: import eia.src.azsynapse as azsyn
except ModuleNotFoundError: import azsynapse as azsyn
try: import eia.src.api_requests as rest_api
except ModuleNotFoundError: import api_requests as rest_api

YUP = "YUP"
YRL = "YRL"
GINP = "EPXXX2"
PROJECT_DIR = path.Path(__file__).parent.parent.parent

class eiaapi_classbuilder():

    def __init__(self, host: str, dataset_dict_list: List):

        def _define_datasets(dataset_dict_list: List) -> List:
            '''
                Forces structure, define as:
                E.g., dataset_dict_list = [{"process":YUP}, {"process":YRL}, {"product":GINP}]
            '''
            dataset = dataset_dict_list
            return dataset
        
        def _define_eia_key() -> str:
            try:
                with open(os.path.join(PROJECT_DIR,"local.settings.json")) as f:
                    data = json.load(f)
                    kv_env = data["Values"]["KEYVAULT_ENV"]
                    b_is_local = data["Values"]["IS_RUNNING_LOCALLY"]
            except FileNotFoundError or KeyError:
                kv_env = os.environ["KEYVAULT_ENV"]
                b_is_local = os.environ["IS_RUNNING_LOCALLY"]
            
            if b_is_local == True:
                az_credential = azure.identity.AzureCliCredential()
            else: 
                az_credential = azure.identity.DefaultAzureCredential()
            secret_client = SecretClient(vault_url=f"https://rti-rspaciq-kv{kv_env}.vault.azure.net",
                                            credential=az_credential)
            eia_key = secret_client.get_secret("EIA-API-KEY")
            return eia_key
        
        self.base_url = "https://api.eia.gov/v2"
        self.dataset = _define_datasets(dataset_dict_list)
        self.eia_key = _define_eia_key()
        self.host = host


class eiaapi_refineryrates(eiaapi_classbuilder):

    def __init__(self, host : str, route : str):
        #  "/petroleum/pnp/wiup/data/?frequency=weekly&data[0]=value&facets[product][]=EPXXX2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

        def _define_datasets():
            dataset = [{"process":YUP}, {"process":YRL}, {"product":GINP}]
            return dataset
        
        dataset_dict_list = _define_datasets()
        super().__init__(host, dataset_dict_list)
        self.route = route


    def get_data(self):
        
        def _return_most_recent_friday__from_date(datetime_day):
            weekday = datetime_day.weekday()
            if weekday < 4: datetime_friday = datetime_day - datetime.timedelta(days=(weekday+3))
            elif weekday == 4: datetime_friday = datetime_day
            else: datetime_friday = datetime_day - datetime.timedelta(days=(weekday-4))
            return datetime_friday
        
        def _dataset_handle_to_endpoints() -> list:
            url = self.base_url + self.route
            endpoint_list = [[f"pnp/wiup/data/?api_key={self.eia_key.value}&frequency=weekly&data[0]=value&facets[{k}][]={v}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=12" for k, v in dataset.items()] for dataset in self.dataset]
            return [f"{url}{endpoint[0]}" for endpoint in endpoint_list]
        
        def _process_into_utilization(df_dict):
            
            def __transform_df_for_azure(df):
                '''
                    Requires percent-utilization,
                    and names.
                '''
                df_transform = pd.DataFrame({})
                for col in df.columns:
                    if col == "period":
                        val = df[col].unique()[0]
                        df_transform["Date"] = pd.Series(val)
                    if col == "area-name":
                        tr_cols = [tr_col.replace(" ", "") for tr_col in df[col].to_list()]
                    if col == "percent-utilization":
                        tr_vals = df[col].to_list()

                for k, v in zip(tr_cols, tr_vals):
                    df_transform[k] = pd.Series(v)
                
                return df_transform[["Date", "U.S.", "PADD3"]]

            df_yup = pd.DataFrame({})
            df_yrl = pd.DataFrame({})
            df_ginp = pd.DataFrame({})

            for k in df_dict.keys():
                if k == YUP: 
                    df_yup = df_dict[k]
                elif k == YRL: 
                    df_yrl = df_dict[k]
                elif k == GINP: 
                    df_ginp = df_dict[k]

            columns = ["period", "duoarea", "area-name", "series-description", "value", "units"]
            df_yup_ = df_yup[columns]
            area_names = ["PADD 3", "U.S."]
            merge_columns = columns[0:3]; columns.append("product-name")
            df_ = df_yrl.merge(right=df_ginp[columns], how="inner", on=merge_columns, suffixes=("", ".ginp"))
            filter_columns = columns.copy(); filter_columns.extend(["series-description.ginp", "value.ginp", "units.ginp"])
            df_eia = df_[filter_columns]
            df_eia = df_eia.drop(labels=["product-name"], axis=1)
            map_names = {"series-description":"output-description", "value":"output-value", "units":"output-units", 
                         "series-description.ginp":"input-description", "value.ginp":"input-value", "units.ginp":"input-units"}
            map = {"output-description":str, "output-value":float, "output-units":str, 
                         "input-description":str, "input-value":float, "input-units":str}
            df_eia = df_eia.rename(columns=map_names)
            df_eia = df_eia.astype(map)
            df_eia["percent-utilization"] = (df_eia["input-value"]/df_eia["output-value"])*100
            df_eia["percent-utilization"] = df_eia["percent-utilization"].round(2)
            
            datetime_friday = _return_most_recent_friday__from_date(datetime.datetime.today()); datestr_friday = datetime_friday.strftime("%Y-%m-%d")
            datetime_prev_friday = datetime_friday - datetime.timedelta(days=7); datestr_prev_friday = datetime_prev_friday.strftime("%Y-%m-%d")
            merge_columns.append("percent-utilization")
            df_eia_fil = df_eia[(df_eia["period"]==datestr_friday) & (df_eia["area-name"].isin(area_names))][merge_columns]
            if df_eia_fil.empty:
                df_eia_fil = df_eia[(df_eia["period"]==datestr_prev_friday) & (df_eia["area-name"].isin(area_names))][merge_columns]
            df_eia_fil.reset_index(drop=True, inplace=True)
            df_eia_final = __transform_df_for_azure(df_eia_fil)
            return df_eia_final

        # Entry:
        # ``````
        endpoint_list, dataset_list = _dataset_handle_to_endpoints(), self.dataset
        responses_dict = rest_api.execute_calls_get_objects(endpoint_list=endpoint_list, dataset=dataset_list)
        df_dict = {}
        for k in responses_dict:
            response_object = responses_dict[k]
            data_records = response_object.json()["response"]["data"]
            df = pd.DataFrame(data=data_records)
            df_dict[k] = df
        
        df = _process_into_utilization(df_dict)

        return df
    
    def upload_eia_data(self, host, df):
        
        def _clean_types(df):
                    # Date Col:
                    df["Date"] = df["Date"].apply(lambda date_str: datetime.datetime.strptime(date_str, ("%Y-%m-%d")))
                    # Other Cols:
                    cols = pd.Series(df.columns)
                    cols = cols[~cols.isin(["Date"])]
                    types = (float for col in cols)
                    frame_types = dict(zip(cols, types))
                    return df.astype(dtype=frame_types)

        def _exec_upsert(az_syn, df, tbl, pk):
            '''
                Upsert data from df into tbl. Performs
                a read on tbl using sql_get_string
                and pk, the primary key.
            '''
            def __clear_zeroes(df):
                df_ = pd.DataFrame({})
                del_list = ['0.0', 'nan', 'None']
                for col in df.columns:
                    try: val = str(float(df[col].iloc[0]))
                    except: val = str(df[col].iloc[0])
                    if val in del_list:
                        pass
                    else:
                        df_[col] = [df[col].iloc[0]]
                return df_

            # Enter:
            df = __clear_zeroes(df)

            Session = sessionmaker(az_syn.engine)
            
            with Session() as session:
                
                # Simple update w/ clear:
                sql_del_string = sa.delete(tbl).compile(
                    dialect=sa.dialects.mssql.pyodbc.dialect(),
                    compile_kwargs={"literal_binds":True}).string
                logging.info(sql_del_string)
                if "dbo" not in sql_del_string: session.execute(sa.text(sql_del_string)) # Safe guard.

                sql_stmt_string = sa.insert(tbl).values(df.to_dict(orient="records")).compile(
                    dialect=sa.dialects.mssql.pyodbc.dialect(),
                    compile_kwargs={"literal_binds":True}).string
                logging.info(sql_stmt_string)
                session.execute(sa.text(sql_stmt_string))
                
                # session.commit() # Set to autocommit for Az Syn.
                session.close()
        
        # Ensure type match:
        df = _clean_types(df)

        # ODBC General authentication:
        driver = "{ODBC Driver 18 for SQL Server}"
        port = 1433
        database = "synapsesqlserver"
        timeout = "30"
        
        # Get Data From Synapse:
        az_syn = azsyn.AzureSynapseInstance(driver=driver, host=host, port=port, database=database, timeout=timeout)
        tbl_stg_RefineryRates = az_syn.get_tbl_stg_RefineryRates()
        pk="Date"
        _exec_upsert(az_syn, df, tbl_stg_RefineryRates, pk)

    def refineryrates_main(self, route):
        self.upload_eia_data(host=self.host, df=self.get_data())
    

BREPUUS = "BREPUUS"
NGHHMCF = "NGHHMCF"
NGHHUUS = "NGHHUUS"
WTIPUUS = "WTIPUUS"

class eiaapi_forecast():

    def __init__(self, host:str, endpoint:str):
        
        def _define_datasets():
            dataset = [{"process":YUP}, {"process":YRL}, {"product":GINP}]
            return dataset
        
        dataset_dict_list = _define_datasets()
        super().__init__(host, dataset_dict_list)
        
        "https://api.eia.gov/v2/steo/data/?frequency=monthly&data[0]=value&facets[seriesId][]=BREPUUS&facets[seriesId][]=NGHHMCF&facets[seriesId][]=NGHHUUS&facets[seriesId][]=WTIPUUS&start=2023-01&end=2026-12&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
        #_dataset_handle_to_endpoints(dataset_dict_list)
        


    def get_data(self):
        
        def _return_most_recent_friday__from_date(datetime_day):
            weekday = datetime_day.weekday()
            if weekday < 4: datetime_friday = datetime_day - datetime.timedelta(days=(weekday+3))
            elif weekday == 4: datetime_friday = datetime_day
            else: datetime_friday = datetime_day - datetime.timedelta(days=(weekday-4))
            return datetime_friday

        def _process_into_utilization(df_dict):
            
            def __transform_df_for_azure(df):
                '''
                    Requires percent-utilization,
                    and names.
                '''
                df_transform = pd.DataFrame({})
                for col in df.columns:
                    if col == "period":
                        val = df[col].unique()[0]
                        df_transform["Date"] = pd.Series(val)
                    if col == "area-name":
                        tr_cols = [tr_col.replace(" ", "") for tr_col in df[col].to_list()]
                    if col == "percent-utilization":
                        tr_vals = df[col].to_list()

                for k, v in zip(tr_cols, tr_vals):
                    df_transform[k] = pd.Series(v)
                
                return df_transform[["Date", "U.S.", "PADD3"]]

            df_yup = pd.DataFrame({})
            df_yrl = pd.DataFrame({})
            df_ginp = pd.DataFrame({})

            for k in df_dict.keys():
                if k == YUP: 
                    df_yup = df_dict[k]
                elif k == YRL: 
                    df_yrl = df_dict[k]
                elif k == GINP: 
                    df_ginp = df_dict[k]

            columns = ["period", "duoarea", "area-name", "series-description", "value", "units"]
            df_yup_ = df_yup[columns]
            area_names = ["PADD 3", "U.S."]
            merge_columns = columns[0:3]; columns.append("product-name")
            df_ = df_yrl.merge(right=df_ginp[columns], how="inner", on=merge_columns, suffixes=("", ".ginp"))
            filter_columns = columns.copy(); filter_columns.extend(["series-description.ginp", "value.ginp", "units.ginp"])
            df_eia = df_[filter_columns]
            df_eia = df_eia.drop(labels=["product-name"], axis=1)
            map_names = {"series-description":"output-description", "value":"output-value", "units":"output-units", 
                         "series-description.ginp":"input-description", "value.ginp":"input-value", "units.ginp":"input-units"}
            map = {"output-description":str, "output-value":float, "output-units":str, 
                         "input-description":str, "input-value":float, "input-units":str}
            df_eia = df_eia.rename(columns=map_names)
            df_eia = df_eia.astype(map)
            df_eia["percent-utilization"] = (df_eia["input-value"]/df_eia["output-value"])*100
            df_eia["percent-utilization"] = df_eia["percent-utilization"].round(2)
            
            datetime_friday = _return_most_recent_friday__from_date(datetime.datetime.today()); datestr_friday = datetime_friday.strftime("%Y-%m-%d")
            datetime_prev_friday = datetime_friday - datetime.timedelta(days=7); datestr_prev_friday = datetime_prev_friday.strftime("%Y-%m-%d")
            merge_columns.append("percent-utilization")
            df_eia_fil = df_eia[(df_eia["period"]==datestr_friday) & (df_eia["area-name"].isin(area_names))][merge_columns]
            if df_eia_fil.empty:
                df_eia_fil = df_eia[(df_eia["period"]==datestr_prev_friday) & (df_eia["area-name"].isin(area_names))][merge_columns]
            df_eia_fil.reset_index(drop=True, inplace=True)
            df_eia_final = __transform_df_for_azure(df_eia_fil)
            return df_eia_final

        # Entry:
        # ``````
        endpoint_list, dataset = "", ""
        responses_dict = rest_api.execute_calls_get_objects(endpoint_list=endpoint_list, dataset=dataset)
        df_dict = {}
        for k in responses_dict:
            response_object = responses_dict[k]
            data_records = response_object.json()["response"]["data"]
            df = pd.DataFrame(data=data_records)
            df_dict[k] = df
        
        df = _process_into_utilization(df_dict)

        return df
    
    def upload_eia_data(self, host, df):
        
        def _clean_types(df):
                    # Date Col:
                    df["Date"] = df["Date"].apply(lambda date_str: datetime.datetime.strptime(date_str, ("%Y-%m-%d")))
                    # Other Cols:
                    cols = pd.Series(df.columns)
                    cols = cols[~cols.isin(["Date"])]
                    types = (float for col in cols)
                    frame_types = dict(zip(cols, types))
                    return df.astype(dtype=frame_types)

        def _exec_upsert(az_syn, df, tbl, pk):
            '''
                Upsert data from df into tbl. Performs
                a read on tbl using sql_get_string
                and pk, the primary key.
            '''
            def __clear_zeroes(df):
                df_ = pd.DataFrame({})
                del_list = ['0.0', 'nan', 'None']
                for col in df.columns:
                    try: val = str(float(df[col].iloc[0]))
                    except: val = str(df[col].iloc[0])
                    if val in del_list:
                        pass
                    else:
                        df_[col] = [df[col].iloc[0]]
                return df_

            # Enter:
            df = __clear_zeroes(df)

            Session = sessionmaker(az_syn.engine)
            
            with Session() as session:
                
                # Simple update w/ clear:
                sql_del_string = sa.delete(tbl).compile(
                    dialect=sa.dialects.mssql.pyodbc.dialect(),
                    compile_kwargs={"literal_binds":True}).string
                logging.info(sql_del_string)
                if "dbo" not in sql_del_string: session.execute(sa.text(sql_del_string)) # Safe guard.

                sql_stmt_string = sa.insert(tbl).values(df.to_dict(orient="records")).compile(
                    dialect=sa.dialects.mssql.pyodbc.dialect(),
                    compile_kwargs={"literal_binds":True}).string
                logging.info(sql_stmt_string)
                session.execute(sa.text(sql_stmt_string))
                
                # session.commit() # Set to autocommit for Az Syn.
                session.close()
        
        # Ensure type match:
        df = _clean_types(df)

        # ODBC General authentication:
        driver = "{ODBC Driver 18 for SQL Server}"
        port = 1433
        database = "synapsesqlserver"
        timeout = "30"
        
        # Get Data From Synapse:
        az_syn = azsyn.AzureSynapseInstance(driver=driver, host=host, port=port, database=database, timeout=timeout)
        tbl_stg_RefineryRates = az_syn.get_tbl_stg_RefineryRates()
        pk="Date"
        _exec_upsert(az_syn, df, tbl_stg_RefineryRates, pk)




if __name__ == "__main__":

    with open(os.path.join(PROJECT_DIR,"local.settings.json")) as f:
        data = json.load(f)
        host = data["Values"]["SYNAPSE_INSTANCE"]
        route = "/petroleum"
    
    eia = eiaapi_refineryrates(host=host, route=route)
    eia.refineryrates_main(route)
