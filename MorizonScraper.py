#Scraping Morizon

#Libraries
from bs4 import BeautifulSoup
from urllib.request import urlopen
from collections import defaultdict
from datetime import datetime
import concurrent.futures
import numpy as np
import pandas as pd
MAX_THREADS = 30
PAGE_NAME = 'https://www.morizon.pl'


class ScrapingMorizon:
    """
    A class used to scrape oferts from morizon.pl

    ...

    Attributes
    ----------
    page : str
        full main page name
    page_name : str
        specific page name which determines if you want to rent or buy/home or apartment etc.
    max_threads : int
        maximum number of threads (default 30)

    Methods
    -------
    enterPage_parser(link):
        Read website, encode and create HTML parser
    extract_links_idClass(isId, to_find,soup, replace, replace_to =[]):
        Extract links with id or class tag
    prepare_range(pages_names):
        Prepare pages range
    flatten(result_to_flatt):
        Flatten a list
    scraping_cities_and_districts_links(page):
        Scraping cities and runnning function to scrape districts
    scraping_districts_links(city_link):
        Scraping cities and districts
    get_districts_cities():
        Get districts links
    scraping_all_links(func, all_links):
        General function to scrape links that activates ThreadPoolExecutor
    scraping_pages_links(district_link):
        Scraping pages links
    get_pages(districts = []):
        The method called up by the user to download all links of the pages from morizon.pl
    scraping_oferts_links(page_link):  
        Scraping oferts links
    get_oferts(pages = []):
        Get districts and cities links
    missed_oferts_pages(self, links, oferts):
        Scrape missed oferts and pages links
    missed_details_func(self, links, oferts):
        Scrape missed details links
    missed_links_all(self, missed_oferts, func, oferts):
        Scrape omitted data until you have scraped all
    join_missed_with_scraped(self, missed, scraped):
        Join missed informations with already scraped
    get_details(self, split_size, oferts = []):
        Get districts and cities links
    scraping_oferts_details_exceptions(link):
        Try to connect with ofert link, if it is not possible save link to global list
    scraping_oferts_details(link):
        Scraping details from ofert
    soup_find_informations(soup, find_attr):
        Find in soup with 3 args
    extract_informations(find_in, find_with_obj = False, obj = None):
        Extract strings from infos founded in soup
    informations_exists(details):
        Verify if basic informations in 'em' tag exists
    spatial_data_exists(data, kind):
        Verify if informations about apartment location exists   
    """

    def __init__(self, page, page_name, max_threads = 30):
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
        
    #Read website, encode and create HTML parser
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
        
        #Get website    
        URL = link
        page = urlopen(URL)
    
        #Read website, encode and create HTML parser
        html_bytes = page.read()
        try:
            html = html_bytes.decode("utf-8")
        except:
            html = html_bytes.decode("latin-1")
            
        return BeautifulSoup(html, "html.parser")
    
    #Extract links with id or class tag
    def extract_links_idClass(self, isId, to_find,soup, replace, replace_to =[]):   
        """Extract links with id or class tag

        extracting links with id or class tag

        Parameters
        ----------
        isId: boolean
            determines whether to look for an id or a class
        to_find: str
            name of class or id  
        soup: BeautifulSoup
            object used to extract informations
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
        
        #Find by id or class
        if(isId):
            extracted = soup.find(id=to_find)
        else:
            extracted = soup.find(class_=to_find)
        
        #If there is only one page assign empty arrays to variables
        try:
            #Find all a tag's
            extracted_names = [name.string for name in extracted.findAll('a') if(name.string != None)]
            #Extract links and replace part of string to create link with newest observations
            extracted_links = [link.get("href") for link in extracted.findAll('a')  if(link.get("href") != None)]
            if(replace):
                extracted_links = [link.replace(replace_to[0], replace_to[1]) for link in extracted_links]
        except:
            extracted_names = []
            extracted_links = []
        
        return extracted_names, extracted_links
    
    #Prepare pages range
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
        
        #if length is 0 then there is only 1 page
        if(len(pages_names) != 0):
            last_page = int(pages_names[len(pages_names) - 1])
        else:
            last_page = 1
        
        return range(1,last_page + 1)
    
    #Flatten a list
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
            if isinstance(i,list): rt.extend(self.flatten(i))
            else: rt.append(i)
        return rt
        
    #Scraping cities and runnning function to scrape districts
    def scraping_cities_and_districts_links(self, page):
        """Scraping cities and runnning function to scrape districts

        Parameters
        ----------
        page: str
            full main page name 
        Returns
        ------
        list
            links to cities
        list
            links to individual city districts
        """
        
        #Read website, encode and create HTML parser
        soup_cities = self.enterPage_parser(page)

        #Extract cities names and links
        cities_names, cities_newest_links = self.extract_links_idClass(isId = True, to_find = 'locationListChildren',soup = soup_cities, replace = True, replace_to = ["mieszkania", "mieszkania/najnowsze"])

        threads = min(self.max_threads, len(cities_newest_links))

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results = list(executor.map(self.scraping_districts_links, cities_newest_links))
            
        return cities_newest_links, results
    
    #Scraping districts links
    def scraping_districts_links(self,city_link):
        """Scraping districts links

        Parameters
        ----------
        city_link: str
            link to specific city
        Returns
        ------
        list
            links to individual city districts
        """
        #Create city link
        link = self.page_name + city_link

        try:
            #Read website, encode and create HTML parser
            soup_districts = self.enterPage_parser(link)
            
            #Extract districts names and links
            districts_names, districs_links = self.extract_links_idClass(isId = True, to_find = 'locationListChildren', soup = soup_districts, replace = True, replace_to = ["mieszkania", "mieszkania/najnowsze"])
        
            districs_newest_links = districs_links
        
        except:
            districs_newest_links = link
            
        return districs_newest_links
            
    #Get districts links
    def get_districts_cities(self):
        """Scraping districts links

        Parameters
        ----------
        city_link: str
            link to specific city
        Returns
        ------
        list
            links to individual city districts
        """
        
        cities_newest_links, results_districts = self.scraping_cities_and_districts_links(self.page)
        results_districts = np.concatenate([districts for districts in results_districts if districts != None], axis=0 )
        
        return results_districts
    
    #General function to scrape links that activates ThreadPoolExecutor
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
    
    #Scraping pages links
    def scraping_pages_links(self, district_link):
        """Scraping pages links

        Parameters
        ----------
        district_link: str
            link to specific district

        Returns
        ------
        list
            scraped pages links
        """
        
        #Create link
        link = self.page_name + district_link
        
        try:
            #Read website, encode and create HTML parser
            soup_pages = self.enterPage_parser(link)
             
            #Extract pages numbers and links
            pages_names, pages_newest_links = self.extract_links_idClass(isId = False, to_find = 'nav nav-pills mz-pagination-number', soup = soup_pages,replace = False)
            pages_range = self.prepare_range(pages_names)
            
            #Create all pages links
            pages_links = [link + '?page=' + str(page) for page in pages_range]
            
            all_pages_links = pages_links
        
        except:
            all_pages_links = link
            
        return all_pages_links
    
    #The method called up by the user to download all links of the pages from morizon.pl
    def get_pages(self, districts = []):
        """The method called up by the user to download all links of the pages from morizon.pl

        Parameters
        ----------
        districts: list, optional
            for which districts the links to the pages are to be downloaded (default for all)

        Returns
        ------
        list
            flatten pages links
        """
        
        #Verify whether user want to specify specific districts
        if any(districts):
            results_districts = districts
        else:
            results_districts = self.get_districts_cities()
        
        results_pages = self.scraping_all_links(self.scraping_pages_links,results_districts)
        results_pages = self.flatten(results_pages)
        
        missed_pages = [oferts for oferts in results_pages if "page" not in oferts]
        
        if len(missed_pages) != 0:
            results_pages = self.flatten([properties for properties in results_pages if (properties != None) & ("page" in properties)])

        missed_pages_list = self.missed_links_all(missed_pages, self.missed_oferts_pages, False,self.scraping_pages_links)
        results_pages = self.join_missed_with_scraped(missed_pages_list,results_pages)

        return self.flatten(results_pages)

    
    #Scraping oferts links
    def scraping_oferts_links(self, page_link):  
        """Scraping oferts links

        Parameters
        ----------
        page_link: str
            link to specific page

        Returns
        ------
        list
            scraped oferts links
        """
        
        try:
            #Read website, encode and create HTML parser
            soup_oferts = self.enterPage_parser(page_link)
               
            properties_links = soup_oferts.findAll(class_ = "property_link")
            properties_links = [link.get("href") for link in properties_links if("oferta" in link.get("href"))]
            
            all_properties_links = properties_links
    
        except:
            all_properties_links = page_link
            
        return all_properties_links
    
    #Get districts and cities links
    def get_oferts(self, pages = []):
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
        
        #Verify whether user want to specify specific pages
        if any(pages):
            results_pages = pages
        else:
            results_pages = self.get_pages()
            
        results_oferts = self.scraping_all_links(self.scraping_oferts_links,results_pages)
        missed_oferts = [oferts for oferts in results_oferts if "page" in oferts]
        results_oferts = np.concatenate([properties for properties in results_oferts if (properties != None) & ("page" not in properties)], axis=0 )

        missed_oferts_list = self.missed_links_all(missed_oferts,self.missed_oferts_pages, True, self.scraping_oferts_links)
        results_oferts = self.join_missed_with_scraped(missed_oferts_list,results_oferts)

        return self.flatten(results_oferts)
    
    #Scrape missed oferts and pages links
    def missed_oferts_pages(self, links, oferts, func):
        """Scrape missed oferts and pages links

        Parameters
        ----------
        links: list
            missing links
        oferts: boolean
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
        
        links = self.scraping_all_links(func,links)
        
        #Assign missed links to variable
        if oferts:
            missed_links = [oferts for oferts in links if "page" in oferts]
        else:
            missed_links = [oferts for oferts in links if "page" not in oferts]
        
        return links, missed_links
    
    #Scrape missed details links
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
        
        links = self.scraping_all_links(self.scraping_oferts_details_exceptions,links)
        
        #Assign missed links to variable
        missed_links = [details for details in links if "www.morizon.pl" in details]
        
        return links, missed_links
    
    #Scrape omitted data until you have scraped all
    def missed_links_all(self, missed_oferts, func, oferts, func_pages_or_oferts):
        """General function to scrape missing links that activates ThreadPoolExecutor until all are scraped
        
        Parameters
        ----------
        missed_oferts: list
            missing links
        func: function
            function which will be activated in ThreadPoolExecutor
        oferts: boolean
            determines whether the missing links relate to properties
        func_pages_or_oferts: function
            function to scrape pages or oferts
            
        Returns
        ------
        list
            scraped elements: details, and links e.g. pages
        """
        
        missed_oferts_list = []
        
        #If there are some missed links left scrape them
        while len(missed_oferts) != 0:
            missed_scraped, missed_oferts = func(missed_oferts, oferts)
            missed_oferts_list.append(missed_scraped)
        
        return missed_oferts_list
            
            
    #Join missed informations with already scraped
    def join_missed_with_scraped(self, missed, scraped):
        """Join missed informations with already scraped
        
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
            scraped = np.concatenate([scraped,missed],axis = 0)
        elif len(missed) == 1:
            scraped = np.concatenate([scraped, missed[0]],axis = 0)
        elif len(missed) == 0:
            scraped = scraped
            
        return scraped
    
    #Get districts and cities links
    def get_details(self, split_size, oferts = []):
        """The method called up by the user to download all details about apartments. Results are saved to number_of_links/split cv files

        Parameters
        ----------
        split_size: int
           value divided by total number of links it is used to create splits to relieve RAM memory 
        oferts: list, optional
            for which oferts links the properties are to be downloaded (default for all)

        """
        
        #Verify whether user want to specify specific pages
        if any(oferts):
            results_oferts = oferts
        else:
            results_oferts = self.get_oferts()
        
        #Create splits to relieve RAM memory
        splitted = np.array_split(list(range(0,len(results_oferts))), len(results_oferts)/split_size)
        splitted = [[elements[0] - 1, elements[-1]] if elements[0] != 0 else [elements[0], elements[-1]]  for elements in splitted]
        splitted[len(splitted) - 1][1] += 1
        
        for split in splitted:
            results_details = self.scraping_all_links(self.scraping_oferts_details_exceptions,results_oferts[split[0]:split[1]])
            
            #Assign to variables missed links and scraped properly
            missed_details = [details for details in results_details if "www.morizon.pl" in details]
            results_details = self.flatten([details for details in results_details if (details != None) & ("www.morizon.pl" not in details)])
            
            #Scrape missed links and join them to already scraped
            missed_details_list = self.missed_links_all(missed_details,self.missed_details_func, True)
            results_details = self.join_missed_with_scraped(missed_details_list,results_details)
            
            #Informations for user
            print("%s splits left" %(len(splitted) - (splitted.index(split) + 1)))
            print("Tyle jest Does not exist: " + str(len([result for result in results_details if result == "Does not exist"])))
            
            #Save scraped details as csv file
            results_details = [result for result in results_details if (result != "Does not exist") & (result != None)]
            pd.DataFrame(results_details).to_csv("mieszkania" + str(split[1]) + ".csv")


    #Try to connect with ofert link, if it is not possible save link to global list
    def scraping_oferts_details_exceptions(self,link):
        """Try to connect with ofert link, if it is not possible save link to global list

        Parameters
        ----------
        link: str
           ofert link
         
        Returns
        ------
        defaultdict
            If scraping succeeds, it is the details of the flat and otherwise a link to the offer
        """
        
        try:
            ofert_infos = self.scraping_oferts_details(link)
        except:
            ofert_infos = link
            
        return ofert_infos
    
    #Scraping details from ofert         
    def scraping_oferts_details(self,link):
        """Try to connect with ofert link, if it is not possible save link to global list

        Parameters
        ----------
        link: str
           ofert link
         
        Returns
        ------
        defaultdict
            the details of the flat 
        str
            Information that offer is no longer available
        """
        
        #Scrapping details from link
        ofert_infos = defaultdict(list)
        soup_details = self.enterPage_parser(link)
        try:
            #Title and subtitle
            title = self.extract_informations(self.soup_find_informations(soup = soup_details,
                                                                   find_attr = ['div', 'class', 'summaryLocation clearfix row']).findAll("span"))
               
            subtitle = self.extract_informations(self.soup_find_informations(soup = soup_details,
                                                                    find_attr = ['div', 'class', 'summaryTypeTransaction clearfix']))
            #Basic informations
            price = self.informations_exists(self.soup_find_informations(soup = soup_details,
                                                                 find_attr = ['li', 'class', 'paramIconPrice']))
            priceM2 = self.informations_exists(self.soup_find_informations(soup = soup_details,
                                                                 find_attr = ['li', 'class', 'paramIconPriceM2']))
            area = self.informations_exists(self.soup_find_informations(soup = soup_details,
                                                                 find_attr = ['li', 'class', 'paramIconLivingArea']))
            rooms = self.informations_exists(self.soup_find_informations(soup = soup_details,
                                                                    find_attr = ['li', 'class', 'paramIconNumberOfRooms']))
               
               
            #Params
            params = soup_details.find(class_ = "propertyParams")
            params_h3 = params.findAll("h3")
            params_tables = params.findAll("table")
            params_p = params.findAll("p")
            
            #Description
            description = self.extract_informations(self.soup_find_informations(soup = soup_details,
                                                                find_attr = ['div', 'class', 'description']),True, "p")
            soup_details.find("section", attrs={"class": "propertyMap"})
            
            #Longitude and Latitude
            google_map = self.soup_find_informations(soup = soup_details, find_attr = ['div', 'class', 'GoogleMap'])
            lat = self.spatial_data_exists(google_map, 'data-lat')
            lng = self.spatial_data_exists(google_map, 'data-lng')
            
            #Assign informations to dictionary
            #ofert_infos["city"] = city_name
            #ofert_infos["district"] = district.split("najnowsze/")[1].split("/")[1]
            ofert_infos["title"] = title
            ofert_infos["subtitle"] = subtitle
            ofert_infos["price"] = price
            ofert_infos["priceM2"] = priceM2
            ofert_infos["area"] = area
            ofert_infos["rooms"] = rooms
            ofert_infos["params_h3"] = params_h3
            ofert_infos["params_tables"] = params_tables
            ofert_infos["params_p"] = params_p
            ofert_infos["description"] = description
            ofert_infos["lat"] = lat
            ofert_infos["lng"] = lng
            ofert_infos["link"] = link
            
            return(ofert_infos)
            
        except:
            return "Does not exist"
        
    #Find in soup with 3 args
    def soup_find_informations(self,soup, find_attr):
        """Find in soup with 3 args

        Parameters
        ----------
        soup: str
            ofert link
        find_attr: list
            attributes of tag
         
        Returns
        ------
        list
            elements with specific attributes
        """
        
        return soup.find(find_attr[0], attrs={find_attr[1]: find_attr[2]})
    
    #Extract strings from infos founded in soup
    def extract_informations(self,find_in, find_with_obj = False, obj = None):
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
            if find_with_obj:
                return [info_part.string.strip() for info_part in find_in.find_all(obj) if(info_part.string != None)]
            else:
                return [info_part.string.strip() for info_part in find_in if(info_part.string != None)]
        except:
            return "None"
    
    #Verify if basic informations in 'em' tag exists
    def informations_exists(self, details):
        """Verify if basic informations in 'em' tag exists

        Parameters
        ----------
        details: list
            elements with specific attributes
            
        Returns
        ------
        list
            elements with specific attributes
        str
            "None" informs that information is not available
        """
        
        try:
            return self.extract_informations(details.em)
        except:
            return "None"
    
    #Verify if informations about apartment location exists    
    def spatial_data_exists(self,data, kind):
        """Verify if informations about apartment location exists    

        Parameters
        ----------
        data: list
            extracted informations about longitude and latitude
        kind: str
            determines whether longitude or latitude is searched   
         
        Returns
        ------
        list
            elements with specific attributes
        str
            "None" informs that information is not available

        """
        
        try:
            return data[kind]
        except:
            return "None"  


morizon_scraper = ScrapingMorizon(page = 'https://www.morizon.pl/do-wynajecia/mieszkania',page_name = 'https://www.morizon.pl',max_threads = 30)
all_oferts = morizon_scraper.get_pages()

now = datetime.now()
print("now =", now)
all_oferts = morizon_scraper.get_pages()
morizon_scraper.get_details(split_size = 500, oferts = all_oferts)
now = datetime.now()
print("now =", now)



        
