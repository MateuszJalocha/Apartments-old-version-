# Add path to scraping scripts
import sys
sys.path.append('Scraping')
sys.path.append('Database_scripts')
sys.path.append('Preprocessing scripts')

from morizonScraper import ScrapingMorizon
from otodomScraper import ScrapingOtodom
from db_manipulation import DatabaseManipulation
from Otodom import Preprocessing_Otodom
import pandas as pd
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
    morizon_scraper = ScrapingMorizon(page = 'https://www.morizon.pl/do-wynajecia/mieszkania',page_name = 'https://www.morizon.pl',max_threads = 30)
    morizon_pages = morizon_scraper.get_pages()
    morizon_offers = morizon_scraper.get_offers(pages=morizon_pages, split_size=100)
    to_scrape = database_manipulation.push_to_database_links(activeLinks=morizon_offers, page_name="Morizon",
                                                             insert_columns=["pageName", "link"])

    morizon_scraped = morizon_scraper.get_details(offers=to_scrape, split_size=500)

    database_manipulation.push_to_database_offers(offers=morizon_scraped,
                                                  insert_columns=["area", "description_1", "description_2",
                                                                  "description_3", "description_4", "latitude", "longitude",
                                                                  "link", "price", "currency", "rooms", "floors_number",
                                                                  "floor", "type_building", "material_building",
                                                                  "year", "headers", "additional_info", "city",
                                                                  "address", "district", "voivodeship", "active",
                                                                  "scrape_date", "inactive_date", "pageName"])

    #Otodom
    otodom_scraper = ScrapingOtodom(page='https://www.otodom.pl/wynajem/mieszkanie/', page_name='https://www.otodom.pl', max_threads=20)

    otodom_pages = otodom_scraper.get_pages()
    otodom_offers = otodom_scraper.get_offers(pages=otodom_pages, split_size=100)
    to_scrape = database_manipulation.push_to_database_links(activeLinks = otodom_offers, page_name = "Otodom",
                                                 insert_columns = ["pageName", "link"])

    to_scrape = pd.read_csv('/home/mateusz/Apartments/dane_otodom_najnowsze/to_scrape.csv')
    otodom_scraped = otodom_scraper.get_details(offers=list(to_scrape["link"]),split_size=500)

    otodom_scraped_c = otodom_scraped.copy().reset_index().drop(['index'],axis=1)

    otodom_scraped_c=otodom_scraped_c.replace({"None": None}).replace({'', None})
    otodom_preprocess = Preprocessing_Otodom(apartment_details=otodom_scraped_c.where(pd.notnull(otodom_scraped_c), None),
                                             information_types=otodom_scraped_c.columns)
    from datetime import datetime
    start_time = datetime.now()
    otodom_table = otodom_preprocess.create_table()
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
    otodom_table=otodom_table.where(pd.notnull(otodom_table), None)
    database_manipulation = DatabaseManipulation(config=config, config_database="DATABASE",
                                                 table_name_links="active_links",
                                                 table_name_offers="preprocessing_offers", split_size=1000)
    database_manipulation.push_to_database_offers(offers=otodom_table,insert_columns=["area", "description_1", "description_2",
                                                                                 "description_3", "description_4","latitude","longitude",
                                                                                 "link", "price", "currency","rooms", "floors_number",
                                                                                 "floor", "type_building", "material_building",
                                                                                 "year", "headers", "additional_info", "city",
                                                                                 "address", "district", "voivodeship", "active",
                                                                                 "scrape_date", "inactive_date", "pageName"])

