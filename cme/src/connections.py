#####################################################
# Author: Jon Willinger
#
# Date: 2024-11-04
#
# Version: 0.0.1
#
# Notes: Connections are made via the Managed 
# Identity. Local development requires az cli; token
# reinjection is automated. The staging and
# production machines require being signed in as the
# same identity/account as is being used in Azure
# Synapse/SQL DB. 
# This may be lifted to Synapse hs.
#
# Behavior changes depending on the env used.
# officepaazure environment is used for powerautomate 
# on RTi-WEB-02\bncadmin. This environment uses creds
# and authenticates differently.
# 
#####################################################

import os
import pandas as pd 
import struct, datetime
import pyodbc, sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, session, sessionmaker
from azure import identity

SQL_COPT_SS_ACCESS_TOKEN = 1256 # As defined in msodbcsql.h

pyodbc.pooling = False


# SQL authentication:
# ``````````````````
class AzConnectSQLAuth():

    def __init__(self, driver, host, port, database, timeout):
        uid = "sqladminuser"
        pwd = ""
        connection_string = f"Driver={driver};Server={host};Port={port};Database={database};Uid={uid};Pwd={pwd};Encrypt=yes;TrustServerCertificate=no;Connection Timeout={timeout};"
        connection_url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string, "autocommit":"true"})
        engine = sa.create_engine(connection_url).execution_options(isolation_level="AUTOCOMMIT")
        self.engine = engine

# Microsoft Entra ID password authentication:
# ````````````````````````````````````````````
class AzConnectMicrosoftEntra():

    def __init__(self, driver, host, port, database, timeout):
        uid = os.environ["AZSYN_UID"] # TODO: Change to flowautomation
        pwd = os.environ["AZSYN_PW"] # TODO: Change to flowautomation
        authentication = "ActiveDirectoryPassword"
        connection_string = f"Driver={driver};Server={host};Port={port};Database={database};Uid={uid};Pwd={pwd};Encrypt=yes;TrustServerCertificate=no;Connection Timeout={timeout}; Authentication={authentication};"
        connection_url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string, "autocommit": "True"})
        engine = sa.create_engine(connection_url).execution_options(isolation_level="AUTOCOMMIT")
        self.engine = engine

# Microsoft Managed Identity:
# ```````````````````````````
# No passwords, more secure. Local dev requires az cli
class AzConnectMI():

    def __init__(self, driver, host, port, database, timeout):
        credential = identity.DefaultAzureCredential()
        connection_string = f"Driver={driver};Server={host};Port={port};Database={database};Encrypt=yes;TrustServerCertificate=no;Connection Timeout={timeout};"
        engine = sa.create_engine(
            sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string, "autocommit":"true"})
        ).execution_options(isolation_level="AUTOCOMMIT")
        self._inject_azure_credential(credential, engine)
        self.engine = engine
        self.credential = credential

    def _inject_azure_credential(self, credential, engine, token_url='https://database.windows.net/'):
        @sa.event.listens_for(engine, 'do_connect')
        def do_connect(dialect, conn_rec, cargs, cparams):
            token = credential.get_token(token_url).token.encode('utf-16-le')
            token_struct = struct.pack(f'=I{len(token)}s', len(token), token)
            attrs_before = cparams.setdefault('attrs_before', {})
            attrs_before[SQL_COPT_SS_ACCESS_TOKEN] = bytes(token_struct)
            return dialect.connect(*cargs, **cparams)


if __name__ == "__main__":
    pass

