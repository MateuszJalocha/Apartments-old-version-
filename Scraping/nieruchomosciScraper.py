# Scraping Otodom

# Libraries
from bs4 import BeautifulSoup
from urllib.request import urlopen
from collections import defaultdict
from datetime import datetime
import concurrent.futures
import numpy as np
import pandas as pd
from scraper import Scraper
from datetime import datetime

z = ["dolnoslaskie", "kujawsko-pomorskie","lodzkie","lubelskie","lubuskie","malopolskie","mazowieckie",
                             "opolskie", "podkarpackie", "podlaskie", "pomorskie", "slaskie", "warminsko-mazurskie",
                             "wielkopolskie","zachodniopomorskie"]

class ScrapingOtodom(Scraper):
    """
    A class used to scrape oferts from otodom.pl

    ...

    Attributes
    ----------
    page : str
        full main page name
    page_name : str
        specific page name which determines if you want to rent or buy/home or apartment etc.
    max_threads : int
        maximum number of threads (default 30)
    voivodeships : list
        list of voivodeships in Poland

    """

    def __init__(self, page, page_name, max_threads=30):
        """
        Parameters
        ----------
        page : str
            full main page name
        page_name : str
            specific page name which determines if you want to rent or buy/home or apartment etc.
        max_threads : int
            maximum number of threads (default 30)
        """

        self.page = page
        self.max_threads = max_threads
        self.page_name = page_name
        self.voivodeships = ["dolnoslaskie"]

    # Scraping pages links
    def scraping_pages_links(self, void):

        # Create link
        link = self.page + void

        try:
            # Read website, encode and create HTML parser
            soup_pages = self.enterPage_parser(link)

            # Extract pages numbers and links
            pages_names, pages_newest_links = self.extract_links_idClass(isId=False,
                                                                         to_find='pager',
                                                                         soup=soup_pages, replace=False)

            pages_range = self.prepare_range(pages_names)

            # Create all pages links
            pages_links = [link + '?page=' + str(page) for page in pages_range]

            all_pages_links = pages_links

        except:
            all_pages_links = link

        return all_pages_links

    # The method called up by the user to download all links of the pages from morizon.pl
    def get_pages(self):

        results_pages = self.scraping_all_links(self.scraping_pages_links, self.voivodeships)
        results_pages = self.flatten(results_pages)

        missed_pages = [oferts for oferts in results_pages if "page" not in oferts]

        if len(missed_pages) != 0:
            results_pages = self.flatten(
                [properties for properties in results_pages if (properties != None) & ("page" in properties)])

        missed_pages_list = self.missed_links_all(missed_offers=missed_pages, func=self.missed_offers_pages,
                                                  details=False, offers=False,
                                                  func_pages_or_offers=self.scraping_pages_links)
        results_pages = self.join_missed_with_scraped(missed_pages_list, results_pages)

        return self.flatten(results_pages)

        return results_pages

    # Scraping offers links
    def scraping_offers_links(self, page_link):
        """Scraping offers links

        Parameters
        ----------
        page_link: str
            link to specific page

        Returns
        ------
        list
            scraped offers links
        """

        try:
            # Read website, encode and create HTML parser
            soup_offers = self.enterPage_parser(page_link)

            properties_links = [art["data-url"] for art in soup_offers.select("article") if art.has_attr("data-url")]

            all_properties_links = properties_links

        except:
            all_properties_links = page_link

        return all_properties_links

    # Get districts and cities links
    def get_offers(self, pages=[]):
        """The method called up by the user to download all links of the properties from morizon.pl

        Parameters
        ----------
        pages: list, optional
            for which pages the links to the properties are to be downloaded (default for all)

        Returns
        ------
        list
            flatten properties links
        """

        # Verify whether user want to specify specific pages
        if any(pages):
            results_pages = pages
        else:
            results_pages = self.get_pages()

        results_offers = self.scraping_all_links(self.scraping_offers_links, results_pages)
        missed_offers = [offers for offers in results_offers if "page" in offers]
        results_offers = np.concatenate(
            [properties for properties in results_offers if (properties != None) & ("page" not in properties)], axis=0)

        missed_offers_list = self.missed_links_all(missed_offers=missed_offers, func=self.missed_offers_pages,
                                                   details=False, offers=True,
                                                   func_pages_or_offers=self.scraping_offers_links)
        results_offers = self.join_missed_with_scraped(missed_offers_list, results_offers)

        return self.flatten(results_offers)

    # Get apartments details
    def get_details(self, split_size, skip_n_elements=0, offers=[]):
        """The method called up by the user to download all details about apartments. Results are saved to number_of_links/split cv files

        Parameters
        ----------
        split_size: int
           value divided by total number of links it is used to create splits to relieve RAM memory
        from: int, default(0)
            how many first "splitted" elements should be omitted
        offers: list, optional
            for which offers links the properties are to be downloaded (default for all)

        """

        # Verify whether user want to specify specific pages
        if any(offers):
            results_offers = offers
        else:
            results_offers = self.get_offers()

        # Create splits to relieve RAM memory
        if (len(results_offers) < split_size):
            splitted = range(0, len(results_offers))
        else:
            splitted = np.array_split(list(range(0, len(results_offers))), len(results_offers) / split_size)
            splitted = [[elements[0] - 1, elements[-1]] if elements[0] != 0 else [elements[0], elements[-1]] for
                        elements in splitted]
            splitted[len(splitted) - 1][1] += 1

        for split in splitted[skip_n_elements:]:
            results_details = self.scraping_all_links(self.scraping_offers_details_exceptions,
                                                      results_offers[split[0]:split[1]])

            # Assign to variables missed links and scraped properly
            missed_details = [details for details in results_details if "www.morizon.pl" in details]
            results_details = self.flatten(
                [details for details in results_details if (details != None) & ("www.morizon.pl" not in details)])

            # Scrape missed links and join them to already scraped
            missed_details_list = self.missed_links_all(missed_offers=missed_details, func=self.missed_details_func,
                                                        restriction=5, details=True)
            results_details = self.join_missed_with_scraped(missed_details_list, results_details)

            # Information for user
            print("%s splits left" % (len(splitted) - (splitted.index(split) + 1)))
            print("Tyle jest Does not exist: " + str(
                len([result for result in results_details if result == "Does not exist"])))

            # Save scraped details as csv file
            results_details = [result for result in results_details if
                               (result != "Does not exist") & (result != None) & ("www.morizon.pl" not in result)]
            pd.DataFrame(results_details).to_csv("mieszkania" + str(split[1]) + "ss.csv")



otodom_pages = ScrapingOtodom(page='https://www.otodom.pl/wynajem/mieszkanie/', page_name='https://www.otodom.pl', max_threads=30)

now = datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
oto_pag = otodom_pages.get_offers()
now = datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
print(len(oto_pag))
