#####################################################
# Author: Jon Willinger
#
# Date: 2025-01-10
#
# Version: 0.0.2
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
try: import eia.src.connections as conn
except ModuleNotFoundError: import connections as conn

class AzureSynapseInstance():
    
    def __init__(self, driver, host, port, database, timeout):

        auth_protocol, azconnection = self._determine_auth_protocol(driver, host, port, database, timeout)
        self.engine = azconnection.engine
        if auth_protocol == "azmi": self.credential = azconnection.credential
        self.connection = self.engine.connect()
        self._Base = self._get_class_Base()

    def _determine_auth_protocol(self, driver, host, port, database, timeout):
        '''
        Returns: 
            "azmi" for managed identity auth.
            "sqlauth" for sqlserver style auth.
            "entra" for Microsoft Entra ID auth.
        '''
        try: py_environ = os.environ["CONDA_DEFAULT_ENV"]
        except: py_environ = "undefined"
        
        if py_environ == "officepaazure":
            # Integrated with PA.
            auth_protocol = "entra"
            azconnection = conn.AzConnectMicrosoftEntra(driver=driver, host=host, port=port, database=database, timeout=timeout)
        elif py_environ == "officeazure":
            # Use MI.
            auth_protocol = "azmi"
            azconnection = conn.AzConnectMI(driver=driver, host=host, port=port, database=database, timeout=timeout)
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

    def get_table_data_from_string_as_df(self, sql_stmt_string): 
        return pd.read_sql(sql=sql_stmt_string, con=self.connection)
    
    def get_table_data_from_tbl_as_df(self, table_class):
        Session = sessionmaker(self.engine)
        with Session() as session:
            data = [tbl.__dict__ for tbl in session.query(table_class).all()]
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
        df_az = self.get_table_data_from_tbl_as_df(tbl)
        pk_sa = pk.lower().replace(" ", "_")
        df_up = df[df[pk].isin(df_az[pk_sa].to_list())]
        df_ins = df[~df[pk].isin(df_az[pk_sa].to_list())]
        return df_up, df_ins

    def _get_class_Base(self):
        class Base(DeclarativeBase):
            pass
        return Base

    def get_tbl_stg_RTiPetchem_SO(self):
        class tbl_stg_RTiPetchem_SO(self._Base):
            __tablename__ = "RTiPetchem_SO" # "Test_RTiPetchem_SO"
            __table_args__ = {"schema": "stg"}
            date: sa.orm.Mapped[str] = mapped_column("Date", sa.String, primary_key=True) # hs string
            ethane: sa.orm.Mapped[str] = mapped_column("Ethane", sa.String, nullable=True)
            spot_ethylene: sa.orm.Mapped[str] = mapped_column("Spot Ethylene", sa.String, nullable=True)
            propane: sa.orm.Mapped[str] = mapped_column("Propane", sa.String, nullable=True)
            spot_rgp: sa.orm.Mapped[str] = mapped_column("Spot RGP", sa.String, nullable=True)
            spot_pgp: sa.orm.Mapped[str] = mapped_column("Spot PGP", sa.String, nullable=True)
            spot_benzene: sa.orm.Mapped[str] = mapped_column("Spot Benzene", sa.String, nullable=True)
            spot_styrene: sa.orm.Mapped[str] = mapped_column("Spot Styrene", sa.String, nullable=True)
            spot_butadiene: sa.orm.Mapped[str] = mapped_column("Spot Butadiene", sa.String, nullable=True)
            wti_crude_oil: sa.orm.Mapped[str] = mapped_column("WTI Crude Oil", sa.String, nullable=True)
            nat_gas: sa.orm.Mapped[str] = mapped_column("Nat. Gas", sa.String, nullable=True)
            brent_crude_oil: sa.orm.Mapped[str] = mapped_column("Brent Crude Oil", sa.String, nullable=True)
            euro_to_usd: sa.orm.Mapped[str] = mapped_column("Euro to $US", sa.String, nullable=True)
            usd_to_cad: sa.orm.Mapped[str] = mapped_column("US to CA$", sa.String, nullable=True)
            vam: sa.orm.Mapped[str] = mapped_column("VAM", sa.String, nullable=True)
            last_updated: sa.orm.Mapped[str] = mapped_column("LastUpdated", sa.String, nullable=True)

        return tbl_stg_RTiPetchem_SO
        
    def get_tbl_stg_RTiPetchem(self):
        class tbl_stg_RTiPetchem(self._Base):
            __tablename__ = "RTiPetchem"
            __table_args__ = {"schema": "stg"}
            date: sa.orm.Mapped[datetime.date] = mapped_column("Date", sa.Date, primary_key=True)
            wti_crude_oil: sa.orm.Mapped[float] = mapped_column("WTI Crude Oil", sa.Float, nullable=True)
            propane: sa.orm.Mapped[float] = mapped_column("Propane", sa.Float, nullable=True)
            brent_crude_oil: sa.orm.Mapped[float] = mapped_column("Brent Crude Oil", sa.Float, nullable=True)
            ethane: sa.orm.Mapped[float] = mapped_column("Ethane", sa.Float, nullable=True)
            nat_gas: sa.orm.Mapped[float] = mapped_column("Nat. Gas", sa.Float, nullable=True)
            brent_crude_oil: sa.orm.Mapped[float] = mapped_column("Brent Crude Oil", sa.Float, nullable=True)
            euro_to_usd: sa.orm.Mapped[float] = mapped_column("Euro to $US", sa.Float, nullable=True)
            usd_to_cad: sa.orm.Mapped[float] = mapped_column("US to CA$", sa.Float, nullable=True)
        
        return tbl_stg_RTiPetchem
        
    def get_tbl_dbo_RTiPetchem(self):
        class tbl_dbo_RTiPetchem(self._Base):
            __tablename__ = "RTiPetchem"
            __table_args__ = {"schema": "dbo"}
            date: sa.orm.Mapped[datetime.date] = mapped_column("Date", sa.Date, primary_key=True)
            ethane: sa.orm.Mapped[float] = mapped_column("Ethane", sa.Float, nullable=True)
            spot_ethylene: sa.orm.Mapped[float] = mapped_column("Spot Ethylene", sa.Float, nullable=True)
            propane: sa.orm.Mapped[float] = mapped_column("Propane", sa.Float, nullable=True)
            spot_rgp: sa.orm.Mapped[float] = mapped_column("Spot RGP", sa.Float, nullable=True)
            spot_pgp: sa.orm.Mapped[float] = mapped_column("Spot PGP", sa.Float, nullable=True)
            spot_benzene: sa.orm.Mapped[float] = mapped_column("Spot Benzene", sa.Float, nullable=True)
            spot_styrene: sa.orm.Mapped[float] = mapped_column("Spot Styrene", sa.Float, nullable=True)
            spot_butadiene: sa.orm.Mapped[float] = mapped_column("Spot Butadiene", sa.Float, nullable=True)
            wti_crude_oil: sa.orm.Mapped[float] = mapped_column("WTI Crude Oil", sa.Float, nullable=True)
            nat_gas: sa.orm.Mapped[float] = mapped_column("Nat. Gas", sa.Float, nullable=True)
            brent_crude_oil: sa.orm.Mapped[float] = mapped_column("Brent Crude Oil", sa.Float, nullable=True)
            euro_to_usd: sa.orm.Mapped[float] = mapped_column("Euro to $US", sa.Float, nullable=True)
            usd_to_cad: sa.orm.Mapped[float] = mapped_column("US to CA$", sa.Float, nullable=True)
            vam: sa.orm.Mapped[float] = mapped_column("VAM", sa.Float, nullable=True)
        
        return tbl_dbo_RTiPetchem

    def get_tbl_stg_RTiContracts(self):
        class tbl_stg_RTiContracts(self._Base):
            __tablename__ = "Contracts" # "Test_Contracts"
            __table_args__ = {"schema": "stg"}
            date: sa.orm.Mapped[str] = mapped_column("Date", sa.String, primary_key=True) # hs string
            ethylene_contract: sa.orm.Mapped[str] = mapped_column("Ethylene Contract", sa.String, nullable=True)
            pgp_contract: sa.orm.Mapped[str] = mapped_column("PGP Contract", sa.String, nullable=True)
            benzene_contract: sa.orm.Mapped[str] = mapped_column("Benzene Contract", sa.String, nullable=True)
            styrene_contract: sa.orm.Mapped[str] = mapped_column("Styrene Contract", sa.String, nullable=True)
            butadiene_contract: sa.orm.Mapped[str] = mapped_column("Butadiene Contract", sa.String, nullable=True)
            oil_gas_ratio: sa.orm.Mapped[str] = mapped_column("Oil: N.Gas", sa.String, nullable=True) # TODO: Not filled, may remove in future.
            last_updated: sa.orm.Mapped[str] = mapped_column("LastUpdated", sa.String, nullable=True)
            
        return tbl_stg_RTiContracts

    def get_tbl_dbo_RTiContracts(self):
        class tbl_dbo_RTiContracts(self._Base):
            __tablename__ = "Contracts"
            __table_args__ = {"schema": "dbo"}
            date: sa.orm.Mapped[datetime.date] = mapped_column("Date", sa.Date, primary_key=True) # hs string
            ethylene_contract: sa.orm.Mapped[float] = mapped_column("Ethylene Contract", sa.Float, nullable=True)
            pgp_contract: sa.orm.Mapped[float] = mapped_column("PGP Contract", sa.Float, nullable=True)
            benzene_contract: sa.orm.Mapped[float] = mapped_column("Benzene Contract", sa.Float, nullable=True)
            styrene_contract: sa.orm.Mapped[float] = mapped_column("Styrene Contract", sa.Float, nullable=True)
            butadiene_contract: sa.orm.Mapped[float] = mapped_column("Ethylene Contract", sa.Float, nullable=True)
            oil_gas_ratio: sa.orm.Mapped[float] = mapped_column("Oil: N.Gas", sa.Float, nullable=True) # TODO: Not filled, may remove in future.
            last_updated: sa.orm.Mapped[datetime.datetime] = mapped_column("LastUpdated", sa.DATETIME, nullable=True)
        
        return tbl_dbo_RTiContracts
    
    def get_tbl_stg_RefineryRates(self):
        class tbl_stg_RefineryRates(self._Base):
            __tablename__ = "RefineryRates"
            __table_args__ = {"schema": "stg"}
            date: sa.orm.Mapped[datetime.date] = mapped_column("Date", sa.Date, primary_key=True)
            us: sa.orm.Mapped[float] = mapped_column("U.S.", sa.Float, nullable=True)
            padd3: sa.orm.Mapped[float] = mapped_column("PADD3", sa.Float, nullable=True)
        
        return tbl_stg_RefineryRates


if __name__ == "__main__":
    pass