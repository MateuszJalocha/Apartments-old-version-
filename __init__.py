from MorizonScraper import ScrapingMorizon
from Database_scripts.db_manipulation import DatabaseManipulation
import configparser
import urllib
from sqlalchemy import create_engine

if __name__ == "__main__":
    morizon_scraper = ScrapingMorizon(page = 'https://www.morizon.pl/do-wynajecia/mieszkania',page_name = 'https://www.morizon.pl',max_threads = 30)

    # Database connection
    config = configparser.ConfigParser()
    config.read('Database_scripts/config.ini')

    database = "DATABASE"
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

    conn = engine.connect()

    conn.execute("INSERT INTO Active_links ([PageName], [Link]) VALUES ('Morizon', 'DUPA')")

    #manipulatedata = DatabaseManipulation(config, "DATABASE", "Active_links")
    #manipulatedata.push_to_database(activeLinks=zmiana["Link"].to_list(), page_name="Morizon",
    #                               insert_columns=["PageName", "Link"], split_size=1000)
