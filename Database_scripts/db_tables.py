#Tables

#libraries
import pyodbc
import numpy as np
import pandas as pd

def create_table(engine, query):
    conn = engine.connect()
    conn.execute(query)
    conn.close()


#Database connection
config = configparser.ConfigParser() 
config.read('Mieszkania/Database_scripts/config.ini')

engine = connect_database(config, "DATABASE")

#Create tables

create_activeLinks = "CREATE TABLE Active_links (LinkID INT IDENTITY PRIMARY KEY,\
                                           PageName NVARCHAR (128) NOT NULL,\
                                           Link NVARCHAR(256) NOT NULL)"

create_table(engine, create_activeLinks)
