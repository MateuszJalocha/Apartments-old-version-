#Tables

#libraries
import pyodbc
import numpy as np
import pandas as pd
import configparser
from sqlalchemy import create_engine
import urllib

def create_table(engine, query):
    conn = engine.connect()
    conn.execute(query)
    conn.close()

def connect_database(config, database):
    """Connect to database
    Parameters
    ----------
    config : ConfigParser
        params to connect with database
    database : str
        database name in config file
    Returns
    ------
    sqlalchemy engine
        connection with database
    """

    params = urllib.parse.quote_plus(
        'Driver=%s;' % config.get(database, 'DRIVER') +
        'Server=%s,1433;' % config.get(database, 'SERVER') +
        'Database=%s;' % config.get(database, 'DATABASE') +
        'Uid=%s;' % config.get(database, 'USERNAME') +
        'Pwd={%s};' % config.get(database, 'PASSWORD') +
        'Encrypt=yes;' +
        'TrustServerCertificate=no;' +
        'Connection Timeout=30;')

    conn_str = 'mssql+pyodbc:///?odbc_connect=' + params
    engine = create_engine(conn_str)

    return engine

#Database connection
config = configparser.ConfigParser() 
config.read('Database_scripts/config.ini')

engine = connect_database(config, "DATABASE")

#Create tables

create_activeLinks = "CREATE TABLE active_links (link_id INT IDENTITY PRIMARY KEY,\
                                           pageName NVARCHAR (128) NOT NULL,\
                                           link NVARCHAR(256) NOT NULL)"

create_preprocessing_offers = "CREATE TABLE preprocessing_offers (offer_id INT IDENTITY PRIMARY KEY," \
"pageName NVARCHAR (128) NOT NULL," \
"area NVARCHAR (128) NOT NULL," \
"description NVARCHAR (2048) NOT NULL," \
"latitude NVARCHAR (128) NOT NULL," \
"longitude NVARCHAR (128) NOT NULL," \
"link NVARCHAR (128) NOT NULL," \
"price NVARCHAR (128) NOT NULL," \
"rooms NVARCHAR (128) NOT NULL," \
"floors_number NVARCHAR (128) NOT NULL," \
"floor NVARCHAR (128) NOT NULL," \
"type_building NVARCHAR (128) NOT NULL," \
"material_building NVARCHAR (128) NOT NULL," \
"year NVARCHAR (128) NOT NULL," \
"headers NVARCHAR (256) NOT NULL," \
"additional_info NVARCHAR (2048) NOT NULL," \
"city NVARCHAR (128) NOT NULL," \
"address NVARCHAR (128) NOT NULL," \
"district NVARCHAR (128) NOT NULL," \
"voivodeship NVARCHAR (128) NOT NULL)"

create_table(engine, create_activeLinks)
create_table(engine, create_preprocessing_offers)
