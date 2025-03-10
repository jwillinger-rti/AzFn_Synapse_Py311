#####################################################
# Author: Jon Willinger
#
# Date: 2024-11-06
#
# Version: 0.0.1
#
# Notes: Behavior changes depending on the env used.
# officepaazure environment used for powerautomate on 
# RTi-WEB-02\bncadmin. This environment uses creds
# and authenticates differently.
# 
#####################################################

import os
import pandas as pd 
import struct, datetime
import pyodbc, sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, session, sessionmaker, mapped_column
from azure import identity
try: import drivers.src.connections as conn
except ModuleNotFoundError: import connections as conn

class AzureSQLDBInstance():

    def __init__(self, driver, host, port, database, timeout, uid="sqladminuser", pwd="", force_sqlauth=False):
        
        auth_protocol, azconnection = self._determine_auth_protocol(driver, host, port, database, timeout, uid, pwd, force_sqlauth)
        self.engine = azconnection.engine
        if auth_protocol == "azmi": self.credential = azconnection.credential
        self.connection = self.engine.connect()
        self._Base = self._get_class_Base()

    def _determine_auth_protocol(self, driver, host, port, database, timeout, uid="sqladminuser", pwd="", force_sqlauth=False):
        '''
        Returns: 
            "azmi" for managed identity auth.
            "sqlauth" for sqlserver style auth.
            "entra" for Microsoft Entra ID auth.
        '''
        try: py_environ = os.environ["CONDA_DEFAULT_ENV"]
        except: py_environ = "undefined"

        if force_sqlauth: py_environ
        
        if py_environ == "officepaazure":
            # Integrated with PA.
            auth_protocol = "entra"
            azconnection = conn.AzConnectMicrosoftEntra(driver=driver, host=host, port=port, database=database, timeout=timeout)
        elif py_environ == "officeazure":
            # Use MI.
            auth_protocol = "azmi"
            azconnection = conn.AzConnectMI(driver=driver, host=host, port=port, database=database, timeout=timeout)
        
        elif py_environ == "sqlauth":
            # Use MI.
            auth_protocol = "sqlauth"
            azconnection = conn.AzConnectSQLAuth(driver=driver, host=host, port=port, database=database, timeout=timeout, uid=uid, pwd=pwd)
        else:
            # Default to MI for now.
            auth_protocol = "azmi"
            azconnection = conn.AzConnectMI(driver=driver, host=host, port=port, database=database, timeout=timeout)
        return auth_protocol, azconnection

    def close_connection(self):
        try: self.connection.close()
        except: pass

    def dispose(self):
        try: self.connection.close()
        except: pass
        self.engine.dispose()

    def _get_class_Base(self):
        class Base(DeclarativeBase):
            pass
        return Base
    
    def get_table_data_from_tbl_as_df(self, table_class, table_class_col_obj, where_val):
        
        Session = sessionmaker(self.engine)
        with Session() as session:
            data = [tbl.__dict__ for tbl in session.query(table_class).where(table_class_col_obj == where_val)]
            session.close()
        
        index = len(data)
        df = pd.DataFrame.from_dict(data=data)
        try:df.drop(columns=["_sa_instance_state"], axis=1, inplace=True)
        except Exception as e: print(e)
        if df.empty:
            print("Empty Dataframe: setting date column.")
            df["date"] = None
        return df
    
    def process_dfs_for_upsert(self, df, tbl, pk):
        '''
            Processes df into two 
            separate dfs for upsert.
        '''
        id_val = df[pk].iloc[0]
        df_az = self.get_table_data_from_tbl_as_df(tbl, tbl.id_, id_val)
        pk_sa = pk.lower().replace(" ", "_")
        df_up = df[df[pk].isin(df_az[pk_sa].to_list())]
        df_ins = df[~df[pk].isin(df_az[pk_sa].to_list())]
        return df_up, df_ins
    
    def get_dbo_tbl_document_driver_historical(self):
        class tbl_document_driver_historical(self._Base):
            __tablename__ = "tblDocumentDriverHistorical"
            __table_args__ = {"schema": "dbo"}
            id_: sa.orm.Mapped[int] = mapped_column("id", sa.Integer, primary_key=True)
            pdfName: sa.orm.Mapped[str] = mapped_column("pdfName", sa.String, nullable=True)
            pdf: sa.orm.Mapped[sa.LargeBinary] = mapped_column("pdf", sa.LargeBinary, nullable=True)
            length: sa.orm.Mapped[int] = mapped_column("length", sa.BigInteger, nullable=True)

        return tbl_document_driver_historical
    
    def get_dbo_tbl_metadata_driver_historical(self):
        class tbl_metadata_driver_historical(self._Base):
            __tablename__ = "tblMetaDataDriverHistorical"
            __table_args__ = {"schema": "dbo"}
            id_: sa.orm.Mapped[int] = mapped_column("id", sa.Integer, primary_key=True)
            documentId: sa.orm.Mapped[int] = mapped_column("documentId", sa.Integer, nullable=True)
            pdfName: sa.orm.Mapped[str] = mapped_column("pdfName", sa.String, nullable=True)
            pages: sa.orm.Mapped[int] = mapped_column("pages", sa.Integer, nullable=True)
            headers: sa.orm.Mapped[str] = mapped_column("headers", sa.String, nullable=True)
            dates: sa.orm.Mapped[datetime] = mapped_column("dates", sa.Date, nullable=True)
        
        return tbl_metadata_driver_historical

if __name__ == "__main__":
    pass