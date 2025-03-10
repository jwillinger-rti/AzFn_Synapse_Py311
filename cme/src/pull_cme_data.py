####################################
# Author: Jon Willinger
# Date: 2024-11-21
# Notes: 
# bloomberg_currencies_url = "https://www.bloomberg.com/markets/api/comparison/data?securities=EURUSD%3ACUR,USDCAD%3ACUR&securityType=CURRENCY&locale=en"
# bloomberg_energy_url = "https://www.bloomberg.com/markets/api/comparison/data?securities=CL1%3ACOM,CO1%3ACOM,NG1%3ACOM&securityType=COMMODITY&locale=en"
####################################

import os, csv, re
import requests, logging
import pathlib as path
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import datetime, tempfile
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa
import pandas as pd, numpy as np
try: import cme.src.azsynapse as azsyn
except ModuleNotFoundError: import azsynapse as azsyn

# CME Datamine does not support OAuth.
FULL_COLUMN_COUNT = 11

_26_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES = "26 Crude Oil Last Day Financial Futures"                    # WTI
B0_MONT_BELVIEU_LDH_PROPANE_OPIS_FUTURES = "B0 Mont Belvieu LDH Propane (OPIS) Futures"                 # Propane
BZ_BRENT_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES = "BZ Brent Crude Oil Last Day Financial Futures"         # Brent
C0_MONT_BELVIEU_ETHANE_OPIS_FUTURES = "C0 Mont Belvieu Ethane (OPIS) Futures"                           # Ethane
C1_CANADIAN_DOLLAR_US_DOLLAR_CAD_USD_FUTURES = "C1 Canadian Dollar/U.S. Dollar (CAD/USD) Futures"       # US to CA
EC_EURO_US_DOLLAR_EUR_USD_FUTURES = "EC Euro/U.S. Dollar (EUR/USD) Futures"
NG_HENRY_HUB_NATURAL_GAS_FUTURES = "NG Henry Hub Natural Gas Futures"


class CMEDatamineAPI:

    def __init__(self):
        self.api_id = "API_RTIGLOBAL2" # Could make a call, but not really sensitive.
        self.api_pw = "QNErr#m94eq$nGHJNmnHTAh7" # Could make a call, but not really sensitive.
        self.base_endpoint = "https://datamine.cmegroup.com/cme/api/v1/download"
    
    def get_dfs_from_fid_dict(self, fid_dict, date=None):
        '''
            Calls download and processes
            the files into dfs for upsert.
            Deletes the file as cleanup.
        '''
        def _process_file_into_df(temp_file_name, data_set):
            
            def __search_string_in_file_get_header_footer(filepath, search_header_str, search_footer_str):
                """Searches for a string in a text file and prints matching lines."""
                
                with open(filepath, "rb") as temp_file:
                    for row_number, line in enumerate(temp_file, 1):
                        line = line.decode("utf8")
                        if search_header_str in line:
                            print(line.strip())
                            header_row_number = row_number

                            for r_n, line2 in enumerate(temp_file, row_number):
                                line2 = line2.decode("utf8")
                                if search_footer_str in line2:
                                    print(line2.strip())
                                    footer_row_number = r_n
                                    break
                            break
                    temp_file.close()
                return header_row_number, footer_row_number
            
            def __extract_subset_file_to_df(file_path, data_set, header_row_number, footer_row_number):
                '''Extract specific subset; simplifies dataframe creation'''

                def ___get_trimmed_line_list(data_set, line):
                    '''
                        This is specific to the dataset.
                        Each new dataset needs its own
                        logic.

                        CSV reader delimiter requires 1 character
                        string, special implementation logic necessary.
                    '''

                    def ____define_null_column_handlers_list(data_set):
                        ''' Parse logic params: 
                            {n1:[(i11, k11), (i12, k12), ...], n2:[(i21, k21), (i22, k22), ...], ...}
                            n = Number of Columns with Data.
                            i_ = Column iterated upon from 0 to len -1.
                            k_ = number of '' columns to add to list
                        '''

                        # Split for customization.
                        if data_set == _26_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == B0_MONT_BELVIEU_LDH_PROPANE_OPIS_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == BZ_BRENT_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == C0_MONT_BELVIEU_ETHANE_OPIS_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == C1_CANADIAN_DOLLAR_US_DOLLAR_CAD_USD_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == EC_EURO_US_DOLLAR_EUR_USD_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == NG_HENRY_HUB_NATURAL_GAS_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        return list_handler
                    
                    # Write data, csv style:
                    list_h = ____define_null_column_handlers_list(data_set) # dataset level
                    line_list = line.split()
                    list_count = len(line_list)

                    # trim with new_line_list:
                    if list_count < FULL_COLUMN_COUNT:
                        new_line_list = []
                        for i_field, field in enumerate(line_list, 0):
                            if list_count not in list_h.keys(): 
                                new_line_list.append(field)
                            else:
                                new_line_list.append(field)
                                for i_col, ncol in list_h[list_count]:
                                    if i_col == i_field:
                                        # doesn't pass on wrong field--good.
                                        for _ in range(ncol): 
                                            new_line_list.append('')
                        line_list = new_line_list
                    else:
                        pass
                    trimmed_line = ",".join(line_list)
                    trimmed_line = f"\n{trimmed_line}"
                    return trimmed_line
                                
                # Write header, csv style:
                with tempfile.NamedTemporaryFile(prefix="output-", suffix=".txt", mode='a', delete=False) as temp_file:
                    temp_file_path = temp_file.name
                
                with open(file_path, 'r') as infile, open(temp_file_path, 'w') as outfile:
                    columns = ["MTH_STRIKE", "DAILY_OPEN", "DAILY_HIGH", "DAILY_LOW", "DAILY_LAST", 
                               "SETT", "PNT_CHGE", "ACT_EST_VOL", "PREV_DAY_SETT", "PREV_DAY_VOL", "PREV_DAY_INT"]
                    for col in columns[:-1]: 
                        col_ = f"{col},"
                        outfile.write(col_)
                    outfile.write(columns[-1])
                    
                    # Write data, csv style:
                    for i_row, line in enumerate(infile):
                        if i_row >= header_row_number and i_row < footer_row_number:
                            print(line)
                            line_list = line.split()
                            trimmed_line = ",".join(line_list)
                            trimmed_line = f"\n{trimmed_line}"

                            trimmed_line = ___get_trimmed_line_list(data_set, line) # trim it.
                            outfile.write(trimmed_line)
                    infile.close()
                    outfile.close()
                outfile = temp_file.name # reinitialize.
                df = pd.read_csv(filepath_or_buffer=temp_file.name, delimiter=',')
                # os.remove(f"Output_{file_path}") # remove extraction file.

                return df
            
            def __clean_inconsistent_columns(df):
                ''' 
                Only Columns 0 to 6 are useable and 
                necessary. Others require more
                extensive parsing.
                '''
                df_ = df.iloc[:, :6]
                return df_

            # Search file
            header, footer = __search_string_in_file_get_header_footer(filepath=temp_file_name, search_header_str=data_set, search_footer_str="TOTAL")
            df = __extract_subset_file_to_df(file_path=temp_file_name, data_set=data_set, header_row_number=header, footer_row_number=footer )
            df = __clean_inconsistent_columns(df)
            return df
        
        # Entry:
        # ``````
        dict_dfs = {}
        for fid, data in fid_dict.items():
            temp_file_name = self.download_and_get_file(fid=fid, date=date)

            for data_set in data:
                df = _process_file_into_df(temp_file_name, data_set)
                dict_dfs[data_set] = df

            # os.remove(file_name)
        return dict_dfs
    
    def download_and_get_file(self, fid, date=None):
        '''
            Receives fid and looks back to previous
            days, accounting for weekends and 
            holidays and downloads most recent
            file.
        '''

        def _execute_call(fid_endpoint):
            # Define the retry strategy.
            retry_strategy = Retry(
                total=4,  # Maximum number of retries.
                status_forcelist=[429, 500, 502, 503, 504],  # the HTTP status codes to retry on.
            )
            
            adapter = HTTPAdapter(max_retries=retry_strategy)
            
            # Create a new session object.
            session = requests.Session()
            session.mount("https://", adapter)

            # Make a request using the session object.
            url = f"{self.base_endpoint}?fid={fid_endpoint}"
            response = session.get(url, auth=(self.api_id, self.api_pw))
            return response, url
        
        def _get_last_business_day(today_datetime, n_past_days):
            # Corrects for Holidays.
            check_datetime = today_datetime - datetime.timedelta(days=n_past_days)
            
            if check_datetime.weekday() == 0:
                n_past_days = n_past_days+3
                last_bus_datetime = today_datetime - datetime.timedelta(days=n_past_days)
            else:
                n_past_days = n_past_days+1
                last_bus_datetime = today_datetime - datetime.timedelta(days=n_past_days)
            return last_bus_datetime
        
        # Entry: 7-day lookback for Holidays.
        if date is not None: today_datetime = date
        else: today_datetime = datetime.datetime.now()

        for i in range(0, 7):
            last_bus_datetime = _get_last_business_day(today_datetime=today_datetime, n_past_days=i)
            fid_date = last_bus_datetime.strftime("%Y%m%d")
            fid_endpoint = f"{fid_date}-{fid}"
            response, url = _execute_call(fid_endpoint)

            if response.status_code == 200:
                # File downloaded successfully.
                fid_datetime = today_datetime.strftime("%Y-%m-%d_%H-%M-%S")
                file_name = "_".join((fid.replace(" ", ""), f"{fid_datetime}"))
                with tempfile.NamedTemporaryFile(prefix=file_name, suffix=".txt", mode="a", delete=False) as temp_file:
                    temp_file_path = temp_file.name
                with open(temp_file_path, 'wb') as f:
                    f.write(response.content)
                    f.close()
                print(f"File, {temp_file_path}, downloaded successfully from {url}.")
                break

            else:
                print("Error:", response.status_code)
                temp_file_path = ""

        return temp_file_path

    def trim_top_month_on_dfs(self, dict_dfs):
        dict_dfs_ = dict()
        for k, df in dict_dfs.items():
            dict_dfs_[k] = df.head(1)
        return dict_dfs_
    
    def concat_dfs_into_sum_df(self, dict_dfs):
        for n, data in enumerate(dict_dfs.items()):
            data[1]["DATA_SET"] = data[0]
            if n == 0: df_sum = data[1]
            else: df_sum = pd.concat([df_sum, data[1]], ignore_index=True)
        return df_sum

    def clean_df(self, df, columns_to_keep):
        '''
            Remove non-numeric columns.
        '''
        
        df = df[columns_to_keep]
        df = df.astype(str)
        for col in df.columns:
            if col not in ["MTH_STRIKE", "DATA_SET"]:
                # df[col] = "0.771A" # test -- good
                df[col] = df[col].str.replace(r'^\.', '0.', regex=True)
                df[col] = df[col].str.extract(r'(\d+\.?\d*)', expand=False).astype(float)
        return df
    
    def transform_df_for_azure_upsert(self, df, date=None):

        def _set_short_names(data_set, date=None):
            def __take_inverse(num: float) -> float:
                if num != 0.0:
                    return 1/num;
                else: return num
            if date is not None: df = pd.DataFrame({"Date":[date.strftime("%Y-%m-%d")]});
            else: df = pd.DataFrame({"Date":[datetime.datetime.now().strftime("%Y-%m-%d")]});
            for n, data in enumerate(data_set):
                if data[1] == "26 Crude Oil Last Day Financial Futures":
                    df["WTI Crude Oil"] = [data[3]] # Settlement
                elif data[1] == "B0 Mont Belvieu LDH Propane (OPIS) Futures":
                    df["Propane"] = [data[3]] # Settlement
                elif data[1]  == "BZ Brent Crude Oil Last Day Financial Futures":
                    df["Brent Crude Oil"] = [data[3]] # Settlement
                elif data[1]  == "C0 Mont Belvieu Ethane (OPIS) Futures":
                    df["Ethane"] = [data[3]] # Settlement
                elif data[1]  == "C1 Canadian Dollar/U.S. Dollar (CAD/USD) Futures":
                    df["US to CA$"] = [__take_inverse(float(data[4]))] # Last
                elif data[1]  == "EC Euro/U.S. Dollar (EUR/USD) Futures":
                    df["Euro to $US"] = [data[4]] # Last
                elif data[1]  == "NG Henry Hub Natural Gas Futures":
                    df["Nat. Gas"] = [data[3]] # Settlement
            return df

        records = df.to_records();
        df = _set_short_names(records, date)
        return df

    def upload_cme_data(self, host, df):
        
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
        tbl_stg_RTiPetchem = az_syn.get_tbl_stg_RTiPetchem()
        pk="Date"
        _exec_upsert(az_syn, df, tbl_stg_RTiPetchem, pk)

    # Batch download endpoint:
    # 'https://datamine.cmegroup.com/cme/api/v1/batchdownload?dataset=eod&yyyymmdd=20241120&period=f'
    url = 'https://datamine.cmegroup.com/cme/api/v1/batchdownload?dataset=eod&yyyymmdd=20241120&period=f'
    

def main(host):

    fid_dict = {"STLBASIC_NYMEX_STLCPC_EOM_0": [_26_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES,
                                                B0_MONT_BELVIEU_LDH_PROPANE_OPIS_FUTURES,
                                                BZ_BRENT_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES,
                                                C0_MONT_BELVIEU_ETHANE_OPIS_FUTURES
                                            ],
                "STLBASIC_SETLCUR_EOM_SUM_0": [C1_CANADIAN_DOLLAR_US_DOLLAR_CAD_USD_FUTURES,
                                            EC_EURO_US_DOLLAR_EUR_USD_FUTURES
                                            ],
                "STLBASIC_NYMEX_EOM_SUM_0": [NG_HENRY_HUB_NATURAL_GAS_FUTURES
                                            ]
    }
    
    cme = CMEDatamineAPI()
    date=None
    # Force here:
    # date_str = "2024-12-16" # The day in the db that needs correction.
    # date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    dict_dfs = cme.get_dfs_from_fid_dict(fid_dict=fid_dict, date=date)
    dict_dfs = cme.trim_top_month_on_dfs(dict_dfs=dict_dfs)
    df = cme.concat_dfs_into_sum_df(dict_dfs)
    df = cme.clean_df(df, ["DATA_SET", "MTH_STRIKE", "SETT", "DAILY_LAST"])
    df.rename(columns={"DATA_SET":"Data_Set", "MTH_STRIKE":"Month", "SETT":"Settlement_Price", "DAILY_LAST":"Last_Price"}, inplace=True)
    df = cme.transform_df_for_azure_upsert(df=df, date=date)
    cme.upload_cme_data(host, df)
    print(df)

if __name__ == "__main__":
    host1 = "rti-synapse-db.sql.azuresynapse.net" # SBX
    host2 = "rti-synapse-pd.sql.azuresynapse.net" # PRD
    # main(host1)
    main(host2)

#  yyyymmdd-dataset_exch_symbol_foi_spread-venue
# payload = open("request.json")
# headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
# r = requests.post(url, data=payload, headers=headers)
