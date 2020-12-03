#Scraping Morizon

#Libraries
from bs4 import BeautifulSoup, SoupStrainer
from urllib.request import urlopen
from requests_html import AsyncHTMLSession, HTMLSession
from collections import defaultdict
from datetime import datetime
import concurrent.futures
import numpy as np
MAX_THREADS = 30
PAGE_NAME = 'https://www.morizon.pl'


class ScrapingMorizon:
    
    def __init__(self,page, page_name, max_threads):
        self.page = page
        self.max_threads = max_threads
        self.page_name = page_name
        
    #Read website, encode and create HTML parser
    def enterPage_parser(self, link):
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
    
    #Extract cities names and links
    def extract_idClass(self, isId, to_find,soup, replace, replace_to =[]):
        
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
        #if length is 0 then there is only 1 page
        if(len(pages_names) != 0):
            last_page = int(pages_names[len(pages_names) - 1])
        else:
            last_page = 1
        
        return range(1,last_page + 1)
    
    #Flatten a list
    def flatten(self, result_to_flatt):
        rt = []
        for i in result_to_flatt:
            if isinstance(i,list): rt.extend(self.flatten(i))
            else: rt.append(i)
        return rt
        
    #Scraping cities and districts
    def scraping_cities_and_districts_links(self, page):
        #Connection with global variable
        global MAX_THREADS
        
        #Read website, encode and create HTML parser
        soup_cities = self.enterPage_parser(page)

        #Extract cities names and links
        cities_names, cities_newest_links = self.extract_idClass(isId = True, to_find = 'locationListChildren',soup = soup_cities, replace = True, replace_to = ["mieszkania", "mieszkania/najnowsze"])

        threads = min(MAX_THREADS, len(cities_newest_links))

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results = list(executor.map(self.scraping_districts_links, cities_newest_links))
            
        return cities_newest_links, results
    
    #Scraping districts links
    def scraping_districts_links(self,city_link):
        #Connection with global variables
        global PAGE_NAME
        
        #Create city link
        link = PAGE_NAME + city_link

        try:
            #Read website, encode and create HTML parser
            soup_districts = self.enterPage_parser(link)
            
            #Extract districts names and links
            districts_names, districs_links = self.extract_idClass(isId = True, to_find = 'locationListChildren', soup = soup_districts, replace = True, replace_to = ["mieszkania", "mieszkania/najnowsze"])
        
            districs_newest_links = districs_links
        
        except:
            districs_newest_links = link
            
        return districs_newest_links
            
    #Get districts and cities links
    def get_districts_cities(self):
        cities_newest_links, results_districts = self.scraping_cities_and_districts_links(self.page)
        results_districts = np.concatenate([districts for districts in results_districts if districts != None], axis=0 )
        
        return results_districts
    
    #General function to scrape links
    def scraping_all_links(self, func, all_links):
        #Connection with global variable
        global MAX_THREADS
        
        threads = min(MAX_THREADS, len(all_links))
           
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results = list(executor.map(func, all_links))
        
        return results
    
    #Scraping pages links
    def scraping_pages_links(self, district_link):
        #Connection with global variable
        global PAGE_NAME
        
        #Create link
        link = PAGE_NAME + district_link
        
        try:
            #Read website, encode and create HTML parser
            soup_pages = self.enterPage_parser(link)
             
            #Extract pages numbers and links
            pages_names, pages_newest_links = self.extract_idClass(isId = False, to_find = 'nav nav-pills mz-pagination-number', soup = soup_pages,replace = False)
            pages_range = self.prepare_range(pages_names)
            
            #Create all pages links
            pages_links = [link + '?page=' + str(page) for page in pages_range]
            
            all_pages_links = pages_links
        
        except:
            all_pages_links = link
            
        return all_pages_links
    
    #Get districts and cities links
    def get_pages(self, districts = []):
        
        #Verify whether user want to specify specific districts
        if any(districts):
            results_districts = districts
        else:
            results_districts = self.get_districts_cities()
        
        results_pages = self.scraping_all_links(self.scraping_pages_links,results_districts)
        results_pages = self.flatten(results_pages)
        
        missed_pages = [oferts for oferts in results_pages if "page" not in oferts]
        
        if len(missed_pages) != 0:
            results_pages = np.concatenate([properties for properties in results_pages if (properties != None) & ("page" in properties)], axis=0 )

        missed_pages_list = self.missed_links_all(missed_pages, self.missed_pages_func)
        results_pages = self.join_missed_with_scraped(missed_pages_list,results_pages)

        return self.flatten(results_pages)

    
    #Scraping oferts links
    def scraping_oferts_links(self, page_link):    
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
        
        #Verify whether user want to specify specific pages
        if any(pages):
            results_pages = pages
        else:
            results_pages = self.get_pages()
            
        results_oferts = self.scraping_all_links(self.scraping_oferts_links,results_pages)
        missed_oferts = [oferts for oferts in results_oferts if "page" in oferts]
        results_oferts = np.concatenate([properties for properties in results_oferts if (properties != None) & ("page" not in properties)], axis=0 )

        missed_oferts_list = self.missed_links_all(missed_oferts,self.missed_oferts_func)
        results_oferts = self.join_missed_with_scraped(missed_oferts_list,results_oferts)

        return self.flatten(results_oferts)
    
    def missed_pages_func(self, links):
        
        links = self.scraping_all_links(self.scraping_oferts_links,links)
        missed_links = [oferts for oferts in links if "page" not in oferts]
        return links, missed_links
    
    def missed_oferts_func(self, links):
        
        links = self.scraping_all_links(self.scraping_oferts_links,links)
        missed_links = [oferts for oferts in links if "page" in oferts]
        return links, missed_links
    
    def missed_links_all(self, missed_oferts, func):
        missed_oferts_list = []
        
        while len(missed_oferts) != 0:
            print(len(missed_oferts))
            missed_scraped, missed_oferts = self.missed_links(missed_oferts)
            missed_oferts_list.append(missed_scraped)
        
        return missed_oferts_list
            
            
    #Join missed informations with already scraped
    def join_missed_with_scraped(self, missed, scraped):

        if len(missed) > 1:
            missed = np.concatenate([properties for properties in missed if properties != None], axis=0)
            scraped = np.concatenate([scraped,missed],axis = 0)
        elif len(missed) == 1:
            scraped = np.concatenate([scraped, missed[0]],axis = 0)
        elif len(missed) == 0:
            scraped = scraped
            
        return scraped


morizon_scraper = ScrapingMorizon('https://www.morizon.pl/do-wynajecia/mieszkania','https://www.morizon.pl',30)

now = datetime.now()
print("now =", now)
all_pages = morizon_scraper.get_pages()
now = datetime.now()
print("now =", now)


now = datetime.now()
print("now =", now)
all_oferts = morizon_scraper.get_oferts()
now = datetime.now()
print("now =", now)

if any([]):
    print("z")
#results_properties = np.concatenate([properties for properties in all_pages if (properties != None) & ("page" not in properties)], axis=0 )

for each in all_pages:
    if len(each) > 1: print(each)

results_oferts = np.concatenate([pages for pages in all_oferts if (pages != None) & (len(pages) == 1)], axis=0)

pipka = []
for each in all_oferts:
    try:
        pipka = np.concatenate([pipka, each], axis=0)
    except:
        print(each)
