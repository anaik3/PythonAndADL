# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 14:24:42 2018

@author: aznaik

This script is reading the csv file from ADl location after authentication.
You will need following packages to make it work:
    1)azure-mgmt-resource
    2)azure-mgmt-datalake-store
    3)azure-datalake-store

"""

import pandas as pd
from azure.datalake.store import core, lib

# Perfoms authentication for accessing Azure Data Lake Store (ADLS)
token = lib.auth()

# Create an ADLS File System Client. The store_name is the name of your ADLS account
adlsFileSystemClient = core.AzureDLFileSystem(token, store_name='wesaprod0adlstore')

# Read a file into pandas dataframe
with adlsFileSystemClient.open('TEST/data.csv', 'rb') as f:
    # Loads a csv with no header
    df = pd.read_csv(f, header=None) 
    
# Show the dataframe
df
