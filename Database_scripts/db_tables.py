#Tables

#libraries
import pyodbc
import numpy as np
import pandas as pd

def create_table(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()

#Database connection
server = 'tcp:apartmentsproject.database.windows.net'
database = 'apartments'
username = 'apartments'
password = 'Pingwin9815$'   
driver= '{ODBC Driver 13 for SQL Server}'

databaseConnection = pyodbc.connect('DRIVER='+driver+';\
                    SERVER='+server+';\
                    PORT=1433;\
                    DATABASE='+database+';\
                    UID='+username+';\
                    PWD='+ password)

#Create tables

create_activeLinks = "CREATE TABLE Active_links (LinkID INT IDENTITY PRIMARY KEY,\
                                           PageName NVARCHAR (128) NOT NULL,\
                                           Link NVARCHAR(256) NOT NULL)"

create_table(databaseConnection, create_activeLinks)
