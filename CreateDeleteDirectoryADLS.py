# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 14:24:42 2018

@author: aznaik

This script creates/deletes directory and upload/download files from local machine to ADLS.
You will need following packages to make it work:
    1)azure-mgmt-resource
    2)azure-mgmt-datalake-store
    3)azure-datalake-store

"""

import pandas as pd
from azure.datalake.store import core, lib, multithread

# Perfoms authentication for accessing Azure Data Lake Store (ADLS)
token = lib.auth()

# Create an ADLS File System Client. The store_name is the name of your ADLS account
adlsFileSystemClient = core.AzureDLFileSystem(token, store_name='wesaprod0adlstore')

# Create a directory in ADLS
adlsFileSystemClient.mkdir('/testDirectoryPython')

# Upload file to created directory
multithread.ADLUploader(adlsFileSystemClient, 
                        lpath='C:\\Users\\aznaik\\Desktop\\PythonADL\\data.csv', 
                        rpath='/testDirectoryPython/data.csv', 
                        nthreads=64, 
                        overwrite=True, 
                        buffersize=4194304, 
                        blocksize=4194304)

# Download file from created directory
multithread.ADLDownloader(adlsFileSystemClient, lpath='C:\\Users\\aznaik\\Desktop\\PythonADL\\data.csv', 
                          rpath='/testDirectoryPython/data.csv', 
                          nthreads=64, 
                          overwrite=True, 
                          buffersize=4194304, 
                          blocksize=4194304)

# Delete directory (removes sub-directories/file recursively)
adlsFileSystemClient.rm('/testDirectoryPython', recursive=True)