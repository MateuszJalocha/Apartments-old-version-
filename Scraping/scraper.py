# Scraping Morizon

# Libraries
from bs4 import BeautifulSoup
from urllib.request import urlopen
from collections import defaultdict
import concurrent.futures
import numpy as np
import pandas as pd


class Scraper:
    # Read website, encode and create HTML parser
    def enterPage_parser(self, link):
        """Read website, encode and create HTML parser

        try to encode with "utf-8" if it creates error then use "laitn-1"

        Parameters
        ----------
        link : str
            link to web page which you want to parse

        Returns
        ------
        BeautifulSoup
            a beautifulsoup object used to extract useful information
        """

        # Get website
        URL = link
        page = urlopen(URL)

        # Read website, encode and create HTML parser
        html_bytes = page.read()
        try:
            html = html_bytes.decode("utf-8")
        except:
            html = html_bytes.decode("latin-1")

        return BeautifulSoup(html, "html.parser")

    # Extract links with id or class tag
    def extract_links_idClass(self, isId, to_find, soup, replace, replace_to=[]):
        """Extract links with id or class tag

        extracting links with id or class tag

        Parameters
        ----------
        isId: boolean
            determines whether to look for an id or a class
        to_find: str
            name of class or id
        soup: BeautifulSoup
            object used to extract information
        replace: boolean
            determines whether part of the link is to be replaced
        replace_to: list
            two elements list containing what [0] has to be replaces with what [1]

        Returns
        ------
        list
            list containing names of extracted links e.g.  districts, cities
        list
            list containing extrated links e.g. districts, pages
        """

        # Find by id or class
        if (isId):
            extracted = soup.find(id=to_find)
        else:
            extracted = soup.find(class_=to_find)

        # If there is only one page assign empty arrays to variables
        try:
            # Find all a tag's
            extracted_names = [name.string for name in extracted.findAll('a') if (name.string != None)]
            # Extract links and replace part of string to create link with newest observations
            extracted_links = [link.get("href") for link in extracted.findAll('a') if (link.get("href") != None)]
            if (replace):
                extracted_links = [link.replace(replace_to[0], replace_to[1]) for link in extracted_links]
        except:
            extracted_names = []
            extracted_links = []

        return extracted_names, extracted_links

    # Prepare pages range
    def prepare_range(self, pages_names):
        """Preparing the range of pages to create links

        Parameters
        ----------
        pages_names: list
            links to individual city districts
        Returns
        ------
        range
            range of pages at morizon for specific page_name
        """

        # if length is 0 then there is only 1 page
        if (len(pages_names) != 0):
            last_page = int(pages_names[len(pages_names) - 1])
        else:
            last_page = 1

        return range(1, last_page + 1)

    # Flatten a list
    def flatten(self, result_to_flatt):
        """Flatten a list

        Parameters
        ----------
        result_to_flatt: list
            which has to be flatten

        Returns
        ------
        list
            flatten list
        """

        rt = []
        for i in result_to_flatt:
            if isinstance(i, list):
                rt.extend(self.flatten(i))
            else:
                rt.append(i)
        return rt

    # General function to scrape links that activates ThreadPoolExecutor
    def scraping_all_links(self, func, all_links):
        """General function to scrape links that activates ThreadPoolExecutor

        Parameters
        ----------
        func: function
            function which will be activated in ThreadPoolExecutor
        all_links: list
            list with links to scrape
        Returns
        ------
        list
            scraped elements: details, and links e.g. pages
        """

        threads = min(self.max_threads, len(all_links))

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results = list(executor.map(func, all_links))

        return results

    # Scrape missed offers and pages links
    def missed_offers_pages(self, links, offers, func):
        """Scrape missed offers and pages links

        Parameters
        ----------
        links: list
            missing links
        offers: boolean
            determines whether the missing links relate to properties
        func: function
            function which will be activated in ThreadPoolExecutor

        Returns
        ------
        list
            scraped missed links
        list
            links that are still missing
        """

        links = self.scraping_all_links(func, links)

        # Assign missed links to variable
        if offers:
            missed_links = [offers for offers in links if "page" in offers]
        else:
            missed_links = [offers for offers in links if "page" not in offers]

        return links, missed_links

    # Scrape omitted data until you have scraped all
    def missed_links_all(self, missed_offers, func, details, restriction=5, offers=None, func_pages_or_offers=None):
        """General function to scrape missing links that activates ThreadPoolExecutor until all are scraped

        Parameters
        ----------
        missed_offers: list
            missing links
        func: function
            function which will be activated in ThreadPoolExecutor
        details: boolean
            determines whether the missing links relate to details
        restriction: int
            restriction for while loop
        offers: boolean, default(None)
            determines whether the missing links relate to properties
        func_pages_or_offers: function, default(None)
            function to scrape pages or offers

        Returns
        ------
        list
            scraped elements: details, and links e.g. pages
        """

        missed_offers_list = []
        n_times = 0

        # If there are some missed links left scrape them
        while len(missed_offers) != 0 & n_times <= restriction:
            if (details):
                missed_scraped, missed_offers = func(missed_offers)
            else:
                missed_scraped, missed_offers = func(missed_offers, offers, func_pages_or_offers)
            missed_offers_list.append(missed_scraped)
            n_times += 1

        return missed_offers_list
        # Join missed information with already scraped
    
    def join_missed_with_scraped(self, missed, scraped):
        """Join missed information with already scraped

        Parameters
        ----------
        missed: list
            scraped missed links
        scraped: list
            links scraped without problems

        Returns
        ------
        list
            scraped elements: details, and links e.g. pages
        """

        if len(missed) > 1:
            missed = np.concatenate([properties for properties in missed if properties != None], axis=0)
            scraped = np.concatenate([scraped, missed], axis=0)
        elif len(missed) == 1:
            scraped = np.concatenate([scraped, missed[0]], axis=0)
        elif len(missed) == 0:
            scraped = scraped

        return scraped

