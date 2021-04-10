# Add path to scraping scripts
import sys
sys.path.append('Scraping')
sys.path.append('Database_scripts')

from morizonScraper import ScrapingMorizon
from otodomScraper import ScrapingOtodom
from db_manipulation import DatabaseManipulation
import configparser
import urllib
from sqlalchemy import create_engine

if __name__ == "__main__":
    # Database connection
    config = configparser.ConfigParser()
    config.read('Database_scripts/config.ini')

    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", split_size = 1000)

    #Morizon
    #morizon_scraper = ScrapingMorizon(page = 'https://www.morizon.pl/do-wynajecia/mieszkania',page_name = 'https://www.morizon.pl',max_threads = 30)

    #Otodom
    otodom_scraper = ScrapingOtodom(page='https://www.otodom.pl/wynajem/mieszkanie/', page_name='https://www.otodom.pl', max_threads=30)
    otodom_offers = otodom_scraper.get_offers()
    to_scrape = database_manipulation.push_to_database_links(activeLinks = otodom_offers, page_name = "Otodom",
                                                 insert_columns = ["pageName", "link"])
    scraped = otodom_offers.get_details(offers=otodom_offers,split_size=500)

    database_manipulation.push_to_database_offers(offers=scraped,insert_columns=["area", "description_1", "description_2", "latitude","longitude",
                                                                                 "link", "price", "currency","rooms", "floors_number",
                                                                                 "floor", "type_building", "material_building",
                                                                                 "year", "headers", "additional_info", "city",
                                                                                 "address", "district", "voivodeship", "active",
                                                                                 "scrape_date", "inactive_date", "pageName"])


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
