# Scraping Otodom
import sys
sys.path.append('Scraping')

# Libraries
from bs4 import BeautifulSoup
from collections import defaultdict
import numpy as np
import pandas as pd
from scraper import Scraper
from datetime import datetime
import json

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
        self.voivodeships = ["dolnoslaskie", "kujawsko-pomorskie","lodzkie","lubelskie","lubuskie","malopolskie","mazowieckie",
                             "opolskie", "podkarpackie", "podlaskie", "pomorskie", "slaskie", "warminsko-mazurskie",
                             "wielkopolskie","zachodniopomorskie"]

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
            for which offers links the properties details are to be scraped (default for all)

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
            missed_details = [details for details in results_details if "www.otodom.pl" in details]
            results_details = self.flatten(
                [details for details in results_details if (details != None) & ("www.otodom.pl" not in details)])

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
                               (result != "Does not exist") & (result != None) & ("www.otodom.pl" not in result)]
            pd.DataFrame(results_details).to_csv("mieszkania" + str(split[1]) + ".csv")

    def json_information_exception(self, obj, path, is_spatial, is_address = False, is_targetFeatures = False, info_type = ''):
        try:
            if is_spatial:
                return self.extract_spatial_information(obj,path)
            elif is_targetFeatures:
                return self.extract_target_features_information(obj, path)
            else:
                return self.extract_localization_information(obj, path, is_address, info_type)
        except:
            return "None"

    def extract_target_features_information(self, obj, path):
        return obj[path[0]][path[1]][path[2]][path[3]][path[4]]

    def extract_localization_information(self, obj, path, is_address, info_type):
        temp_obj = obj[path[0]][path[1]][path[2]][path[3]][path[4]][0][path[5]]

        if is_address:
            return temp_obj
        else:
            return [el['label'] for el in temp_obj if el['type'] == info_type]

    def extract_spatial_information(self, obj, path):
        return obj[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]]

    def extract_information_otodom(self, find_in, is_description=False):
        """Find in soup with 3 args

        Parameters
        ----------
        find_in: BeautifulSoup
            object where used to find information
        find_with_obj: boolean, (default False)
            determines whether user wants to find elements by "obj"
        obj: str, (default None)
            find all elements with that object

        Returns
        ------
        list
            elements with specific attributes
        str
            "None" informs that information is not available
        """

        try:
            if is_description:
                [elem.replace_with(elem.text + "\n\n") for element in find_in for elem in
                 element.find_all(["a", "p", "div", "h3", "br", "li"])]
                return [element.text for element in find_in]
            else:
                return [element.text for element in find_in if element.text != '']
        except:
            return "None"

    # Scraping details from offer
    def scraping_offers_details(self, link):
        """Try to connect with offer link, if it is not possible save link to global list

        Parameters
        ----------
        link: str
           offer link

        Returns
        ------
        defaultdict
            the details of the flat
        str
            Information that offer is no longer available
        """

        # Scraping details from link
        offer_infos = defaultdict(list)
        soup_details = self.enterPage_parser(link)

        try:
            # Title and subtitle
            title = self.extract_information(self.soup_find_information(soup=soup_details,
                                                              find_attr=['h1', 'class',
                                                                         'css-46s0sq edo911a18']))

            subtitle = self.extract_information(self.soup_find_information(soup=soup_details,
                                                                 find_attr=['a', 'class',
                                                                            'css-1qz7z11 eom7om61']))
            price = self.extract_information(self.soup_find_information(soup=soup_details,
                                                              find_attr=['strong', 'class',
                                                                         'css-srd1q3 edo911a17']))

            # Details and description (h2)
            details = self.extract_information_otodom(self.soup_find_information(soup=soup_details,
                                                                       find_attr=['div', 'class',
                                                                                  'css-1d9dws4 e1dlfs272']))
            description = self.extract_information_otodom(soup_details.findAll("p").copy(), True)

            # Additional information (h3)
            additional_info_headers = [header.text for header in soup_details.findAll("h3")]
            additional_info = self.extract_information_otodom(
                soup_details("ul", attrs=["class", "css-13isnqa e9d1vc80"]).copy(), True)

            # Information in json
            try:
                res = soup_details.findAll('script')
                lengths = [len(str(el)) for el in res]
                json_object = json.loads(res[lengths.index(max(lengths))].contents[0])

                # Longitude and Latitude
                lat = self.json_information_exception(obj=json_object,
                                                path=['props', 'pageProps', 'ad', 'location', 'coordinates', 'latitude'],
                                                is_spatial=True)
                lng = self.json_information_exception(obj=json_object,
                                                path=['props', 'pageProps', 'ad', 'location', 'coordinates', 'longitude'],
                                                is_spatial=True)

                # Adress and voivodeship
                address = self.json_information_exception(obj=json_object,
                                                    path=['props', 'pageProps', 'ad', 'location', 'address', 'value'],
                                                    is_spatial=True, is_address=True)
                voivodeship = self.json_information_exception(obj=json_object,
                                                        path=['props', 'pageProps', 'ad', 'location', 'geoLevels', 'label'],
                                                        is_spatial=False, info_type="region")
                city = self.json_information_exception(obj=json_object,
                                                 path=['props', 'pageProps', 'ad', 'location', 'geoLevels', 'label'],
                                                 is_spatial=False, info_type="city")
                district = self.json_information_exception(obj=json_object,
                                                 path=['props', 'pageProps', 'ad', 'location', 'geoLevels', 'label'],
                                                 is_spatial=False, info_type="district")

                # Target features (area, building floors num, etc.)
                features = ["Area", "Build-year", "Building_floors_num", "Building_material", "Building_type",
                            "Construction_status", "Deposit", "Floor_no", "Heating", "Rent", "Rooms_num"]
                values = []

                for feature in features:
                    offer_infos[feature] = self.json_information_exception(obj=json_object,
                                                                           path=['props', 'pageProps', 'ad', 'target',
                                                                                 feature],
                                                                           is_spatial=False, is_targetFeatures=True)


            except:
                features = ["Area", "Build-year", "Building_floors_num", "Building_material", "Building_type",
                            "Construction_status", "Deposit", "Floor_no", "Heating", "Rent", "Rooms_num"]
                lat = "None"
                lng = "None"
                address = "None"
                voivodeship = "None"
                district = "None"
                for feature in features:
                    offer_infos[feature] = "None"

            # Assign information to dictionary
            offer_infos["city"] = city
            offer_infos["district"] = district
            offer_infos["address"] = address
            offer_infos["voivodeship"] = voivodeship
            offer_infos["title"] = title
            offer_infos["subtitle"] = subtitle
            offer_infos["price"] = price
            offer_infos["additional_info_headers"] = additional_info_headers
            offer_infos["additional_info"] = additional_info
            offer_infos["details"] = details
            offer_infos["description"] = description
            offer_infos["lat"] = lat
            offer_infos["lng"] = lng
            offer_infos["link"] = link

            return (offer_infos)

        except:
            return "Does not exist"

    # Scrape missed details links
    def missed_details_func(self, links):
        """Scrape missed details links

        Parameters
        ----------
        links: list
            missing links

        Returns
        ------
        list
            scraped missed links
        list
            links that are still missing
        """

        links = self.scraping_all_links(self.scraping_offers_details_exceptions, links)

        # Assign missed links to variable
        missed_links = [details for details in links if "www.otodom.pl" in details]

        return links, missed_links



otodom_pages = ScrapingOtodom(page='https://www.otodom.pl/wynajem/mieszkanie/', page_name='https://www.otodom.pl', max_threads=30)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
oferty = otodom_pages.get_offers()
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
oto_pag = otodom_pages.get_details(500, offers=oferty)
now = datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
print(len(oto_pag))

