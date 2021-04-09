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
"area FLOAT (2)," \
"description NVARCHAR (4000)," \
"latitude FLOAT," \
"longitude FLOAT," \
"link NVARCHAR (128)," \
"price FLOAT (2)," \
"currency NVARCHAR(128)," \
"rooms FLOAT," \
"floors_number FLOAT," \
"floor FLOAT," \
"type_building NVARCHAR (128)," \
"material_building NVARCHAR (128)," \
"year FLOAT," \
"headers NVARCHAR (256)," \
"additional_info NVARCHAR (4000)," \
"city NVARCHAR (128)," \
"address NVARCHAR (128)," \
"district NVARCHAR (128)," \
"voivodeship NVARCHAR (128)," \
"active NVARCHAR (128)," \
"scrape_date NVARCHAR (128)," \
"inactive_date NVARCHAR (128)," \
"pageName NVARCHAR (128))"

create_missing_links = "CREATE TABLE missing_links (link_id INT IDENTITY PRIMARY KEY," \
"pageName NVARCHAR (128) NOT NULL," \
"link NVARCHAR (128) NOT NULL," \
"link_type NVARCHAR (128) NOT NULL)"

create_table(engine, create_activeLinks)
conn.execute("DROP TABLE preprocessing_offers")
create_table(engine, create_preprocessing_offers)
create_table(engine, create_missing_links)
