#Database manipulation 

#libraries
import pyodbc
import sqlalchemy as sa
from sqlalchemy import create_engine
import urllib
import numpy as np
import pandas as pd
import configparser #configuration from ini file

class DatabaseManipulation:
    """
    A class used to manage apartments database
    ...

    Attributes
    ----------
    config : ConfigParser
            params to connect with database

    Methods
    -------
    connect_database(config, config_database, table_name):
        Connect to database
    create_split(dataFrame, split_size):
        Create splits to make it possible to insert or delete big datasets
    insert_active_links(dataFrame, column_names, split_size = 1000):
        Insert active links to database
    find_links_to_scrape(activeLinks, page_name, split_size = 1000):
        Find new links to scrape and inactive to remove
    replace_links(newLinks, removeLinks, page_name, insert_columns, split_size = 1000):
        Add new links to database and remove inactive
    push_to_database(activeLinks, page_name, insert_columns, split_size = 1000):
        Activate functions to replace and remove observations
    """
    
    def __init__(self, config, config_database,table_name):
        """
        Parameters
        ----------
        config : ConfigParser
            params to connect with database
        config_database : str
            name of database in config file
        table_name : str
            name of table which user want to use 
        """
        
        self.config = config
        self.config_database = config_database
        self.table_name = table_name
        self.engine = self.connect_database(config, config_database)
        
    #Connect to database
    def connect_database(self, config, database):
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
    
    #Create splits to make it possible to insert or delete big datasets
    def create_split(self, dataFrame, split_size):
        """Create splits to make it possible to insert or delete big datasets

        if dataFrame has has smaller number of rows than split_size value than assign as a split size dataFrame number of rows

        Parameters
        ----------
        dataFrame : pd.DataFrame
            data frame with observations
        split_size : int
            size of splits

        Returns
        ------
        list
            list of splits
        """
        
        #If dataFrame has has smaller number of rows than split_size value than assign as a split size dataFrame number of rows
        if(len(dataFrame) < split_size):
            splitted = [[0, len(dataFrame)]]
        else:
            splitted = np.array_split(list(range(0,len(dataFrame))), len(dataFrame)/split_size)
            splitted = [[elements[0] - 1, elements[-1]] if elements[0] != 0 else [elements[0], elements[-1]]  for elements in splitted]
            splitted[len(splitted) - 1][1] += 1
            
        return splitted
    
    #Insert active links to database
    def insert_active_links(self, dataFrame, column_names, split_size = 1000):
        """Insert active links to database

        if column names are correct insert observations to table

        Parameters
        ----------
        dataFrame : pd.DataFrame
            data frame with observations
        column_names : list
            column names of table in which you want to insert new observations (without index column)
        split_size : int
            size of splits

        Returns
        ------
        str
            information that names are incorrect
        """
        #Create splits to insert big dataset
        splitted = self.create_split(dataFrame, split_size)
        
        #Verify corectness of column names
        if sum(dataFrame.columns == column_names) == len(column_names):
            for split in splitted:
                conn = self.engine.connect()
        
                #Add observations
                for index, row in dataFrame[split[0]:split[1]].iterrows():
                    conn.execute("INSERT INTO "+self.table_name+"\
                                 ([PageName],[Link])\
                                 Values (?, ?)\
                                 ",(row["PageName"], row["Link"]))
                conn.close()
        else:
            return "Add correct column names"
    
    #Find new links to scrape and inactive to remove
    def find_links_to_scrape(self, activeLinks, page_name, split_size = 1000):
        """Find new links to scrape and inactive to remove

        Parameters
        ----------
        activeLinks : list
            list of scraped links (offers are available at webpage)
        page_name : str
            name of the website from which data were scraped
        split_size : int
            size of splits

        Returns
        ------
        list
            links that have to be scraped
        list
            links that are no longer available at webpage
        """
        
        #Select active links from database
        conn = self.engine.connect()
        links_database = pd.DataFrame.from_records(conn.execute("SELECT * FROM "+self.table_name+" WHERE [PageName] LIKE '"+page_name+"'").fetchall())
        
        #Find links to scrape and remove
        activeLinks = pd.DataFrame({"Link": activeLinks})
        to_scrape = activeLinks[~activeLinks.stack().isin(links_database.iloc[:,2]).unstack()].dropna()
        to_remove = links_database.iloc[:,2][~links_database.iloc[:,2].isin(activeLinks["Link"])].dropna()
        conn.close()
    
        return to_scrape, to_remove
     
    #Add new links to database and remove inactive
    def replace_links(self, newLinks, removeLinks, page_name, insert_columns, split_size = 1000):
        """Find new links to scrape and inactive to remove

        Parameters
        ----------
        newLinks : list
            List of links to be inserted to the table
        removeLinks : list
            List of links to be removed from the table
        page_name : str
            name of the website from which data were scraped
        insert_columns : list
            column names of table in which you want to insert new observations (without index column)
        split_size : int
            size of splits

        """
        
        #Delete links
        conn = self.engine.connect()
        queries_delete = "DELETE FROM "+self.table_name+" WHERE [Link] = '"+removeLinks+"'"
    
        for query in queries_delete:
            conn.execute(query)
            
        #Insert links
        newLinks = pd.DataFrame({"PageName": page_name, "Link": newLinks["Link"]})
        self.insert_active_links(dataFrame = newLinks, column_names = insert_columns, split_size = split_size)
    
        conn.close()
        
    #Activate functions to replace and remove observations
    def push_to_database(self, activeLinks, page_name, insert_columns, split_size = 1000):
        """Activate functions to replace and remove observations

        Parameters
        ----------
        activeLinks : list
            list of scraped links (offers are available at webpage)
        page_name : str
            name of the website from which data were scraped
        insert_columns : list
            column names of table in which you want to insert new observations (without index column)
        split_size : int
            size of splits

        """
        
        #Find which links has to be scraped and which to removed
        scrape, remove = self.find_links_to_scrape(activeLinks = activeLinks, page_name = page_name, split_size = split_size)
        
        #Delete and replace links
        self.replace_links(newLinks = scrape, removeLinks = remove, page_name = page_name, insert_columns = insert_columns, split_size = split_size)
        
#Database connection
config = configparser.ConfigParser() 
config.read('Mieszkania/Database_scripts/config.ini')

manipulatedata = DatabaseManipulation(config, "DATABASE", "Active_links")
manipulatedata.push_to_database(activeLinks = zmiana["Link"].to_list(), page_name = "Morizon", insert_columns = ["PageName", "Link"],split_size = 1000)
