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
from azure.keyvault.secrets import SecretClient
import connections as conn
import azsql
import process_pdf as proc_pdf

PROJECT_DIR = path.Path(__file__).parent.parent.parent

class driver_pdfs():

    def __init__(self):
        
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
        self.azsqldriver_uid = secret_client.get_secret("AZSQLDriverUID").value
        self.azsqldriver_pw = secret_client.get_secret("AZSQLDriverPW").value
        self.adls_conn_string = secret_client.get_secret("adls-conn-string-key01").value
        self.storage_account_name_for_synapse = storage_account_name_for_synapse
        self.storage_account_key_for_synapse = storage_account_key_for_synapse 
        self.container_name = "rti-synapse-db"
        self.adls_svc_client = BlobServiceClient.from_connection_string(conn_str=self.adls_conn_string, credential=az_credential)
        self.container_client = self.adls_svc_client.get_container_client(container=self.container_name)
        self.az_sqldb = self.handle_az_sqldb(action="get")
        self.TBL_DOCUMENT_DRIVER_HISTORICAL = self.az_sqldb.get_dbo_tbl_document_driver_historical()
        self.TBL_METADATA_DRIVER_HISTORICAL = self.az_sqldb.get_dbo_tbl_metadata_driver_historical()

    def __del__(self): 
        self.handle_az_sqldb(action="close", az_sqldb=self.az_sqldb)

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

    def handle_az_sqldb(self, action="get", az_sqldb=None):
        if action == "get":
            driver = "{ODBC Driver 18 for SQL Server}"
            port = 1433
            host = "rtiglobal.database.windows.net"
            database = "RTiWeb-DEV"
            timeout = "30"
            
            az_sqldb = azsql.AzureSQLDBInstance(driver=driver, host=host, 
                port=port, database=database, timeout=timeout, uid="", pwd="", force_sqlauth=False)
            
            az_sqldb.close_connection
        else:
            az_sqldb.close_connection
        return az_sqldb

    def upload_drivers(self, df, az_sqldb):

        def _execute_upsert(az_sqldb, df, tbl, pk):
            '''
                Upsert data from df into tbl. Performs
                a read on tbl using sql_get_string
                and pk, the primary key.
            '''

            blob_name = df["pdfName"].iloc[0]
            df_azsql = az_sqldb.get_table_data_from_tbl_as_df(tbl, tbl.pdfName, blob_name)
            if df_azsql.empty: 
                df_azsql = pd.DataFrame(data={f"{pk}_":[None], "pdfName":[blob_name]})
            df = df.merge(right=df_azsql[[f"{pk}_", "pdfName"]], left_on="pdfName", right_on="pdfName", 
                        suffixes=(".file", ".db"))
            df_up = df[~df[f"{pk}_"].isin([None])]
            df_ins = df[df[f"{pk}_"].isin([None])]
            
            Session = sessionmaker(az_sqldb.engine)
            with Session() as session:
                
                # Simple update.
                if not(df_up.empty): 
                    sql_stmt_string = "".join(["DECLARE @pdf VARBINARY(MAX) SELECT @pdf = BulkColumn ",
                        f"FROM OPENROWSET(BULK N'{blob_name}', ",
                        "DATA_SOURCE = 'extHistoricalDriversBlob', "
                        "SINGLE_BLOB) AS DOCUMENT; ",
                        "UPDATE [dbo].[tblDocumentDriverHistorical] ",
                        "SET "
                        f"[pdfName] = '{blob_name}'",
                        ", [pdf] = @pdf",
                        ", [length] = DATALENGTH(@pdf)",
                        "FROM [dbo].[tblDocumentDriverHistorical] ",
                        f"WHERE [pdfName] = '{blob_name}'"
                        ])
                    # TODO: logger.info(sql_stmt_string)
                    session.execute(sa.text(sql_stmt_string))

                # Simple insert.
                if not(df_ins.empty):
                    sql_stmt_string = "".join(["DECLARE @pdf VARBINARY(MAX) SELECT @pdf = BulkColumn ",
                        f"FROM OPENROWSET(BULK N'{blob_name}', ",
                        "DATA_SOURCE = 'extHistoricalDriversBlob', "
                        "SINGLE_BLOB) AS DOCUMENT; ",
                        "INSERT INTO [dbo].[tblDocumentDriverHistorical] (",
                        "[pdfName]",
                        ", [pdf]",
                        ", [length]",
                        ") VALUES(",
                        f"'{blob_name}'",
                        ", @pdf",
                        ", DATALENGTH(@pdf)",
                        ")"
                        ])
                    # TODO: logger.info(sql_stmt_string)
                    session.execute(sa.text(sql_stmt_string))
        
        pk = "id"
        tbl_document_driver_historical = self.TBL_DOCUMENT_DRIVER_HISTORICAL
        _execute_upsert(az_sqldb, df, tbl=tbl_document_driver_historical, pk=pk)

    def upload_meta_data(self, df, az_sqldb):
        '''
            Top, one entry is evaluated per dataframe. Limitation.
        '''

        def _get_driver_doc_data(df, az_sqldb):
            bExit = False
            tbl = self.TBL_DOCUMENT_DRIVER_HISTORICAL
            pdf_Name = df["pdfName"].unique()[0]
            df_az = az_sqldb.get_table_data_from_tbl_as_df(tbl, tbl.pdfName, pdf_Name)
            if df_az.empty:
                bExit = True
            else:
                df = df.merge(right=df_az[[f"id_", "pdfName"]], left_on="pdfName", 
                            right_on="pdfName", suffixes=(".meta", ".doc"))
                df.rename(columns={"id_": "documentId"}, inplace=True);

            return df, bExit

        def _exec_upsert(df, az_sqldb):
            # Check if in table: decide upsert:
            
            pk = int(df["documentId"].iloc[0])
            tbl = self.TBL_METADATA_DRIVER_HISTORICAL
            df_ins = df
            Session = sessionmaker(az_sqldb.engine)
            with Session() as session:
                
                # Simple delete.
                # logger.info(df_up.to_dict(orient="records"))
                session.connection().execute(sa.delete(tbl).where(tbl.documentId.in_([pk])))
                
                # Simple insert.
                sql_stmt_string = sa.insert(tbl).values(df_ins.to_dict(orient="records")).compile(
                    dialect=sa.dialects.mssql.pyodbc.dialect(),
                    compile_kwargs={"literal_binds":True}).string
                # logger.info(sql_stmt_string)
                session.execute(sa.text(sql_stmt_string))
                
                # session.commit() # Set to autocommit for Az Syn.
                session.close()


        df, b_exit = _get_driver_doc_data(df, az_sqldb)

        if not(b_exit):
            _exec_upsert(df, az_sqldb)
        else:
            pass
    
    def upload_blob_to_azure(self, source_blob: BlobServiceClient):
        source_blob_name = source_blob.name
        destination_blob_client = self.container_client.get_blob_client(blob=f"drivers-historical-pdfs{source_blob_name[source_blob_name.find('/'):]}")
        destination_blob_client.upload_blob_from_url(source_url=f"{self.container_client.url}/{source_blob_name}", overwrite=True)
        self.container_client.delete_blob(blob=source_blob)

    def bulk_load_pdfs(self, az_sqldb, folder_name):

        blob_list = self.container_client.list_blobs(name_starts_with=folder_name)
        
        for blob in blob_list:
        
            if blob.name.endswith(".pdf"):
                blob_name = blob.name
                self.upload_drivers(pd.DataFrame(data={"pdfName":[blob_name]}, columns=["pdfName"]), az_sqldb)
                blob_client = self.container_client.get_blob_client(blob_name)
                blob_data = blob_client.download_blob().readall()
                file_name = blob.name.split("/")[-1][:-4]
                with tempfile.NamedTemporaryFile(prefix=file_name, suffix=".txt", mode="a", delete=False) as temp_file:
                    temp_file_path = temp_file.name
                    with open(temp_file_path, 'wb') as f:
                        f.write(blob_data)
                        f.close()
                with tempfile.NamedTemporaryFile(prefix=file_name, suffix="_output.md", mode="a", delete=False) as temp_file:
                    output_file = temp_file.name
                df = proc_pdf.process_pdf_return_data(temp_file_path, output_file, blob_name)
                self.upload_meta_data(df=df, az_sqldb=az_sqldb)
                self.upload_blob_to_azure(blob)

    def main(self):
        
        folder_name = "drivers-current-pdfs"
        self.bulk_load_pdfs(self.az_sqldb, folder_name)
    
if __name__ == "__main__":

    dpdf = driver_pdfs()
    dpdf.main()