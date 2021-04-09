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

    def __init__(self, config, config_database, table_name_links, table_name_offers, split_size):
        """
        Parameters
        ----------
        config : ConfigParser
            params to connect with database
        config_database : str
            name of database in config file
        table_name_links : str
            name of table with active links
        table_name_offers : str
            name of table with offers
        split_size : int
            size of splits
        """

        self.config = config
        self.config_database = config_database
        self.table_name_links = table_name_links
        self.table_name_offers = table_name_offers
        self.split_size = split_size
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
    def create_split(self, dataFrame):
        """Create splits to make it possible to insert or delete big datasets

        if dataFrame has has smaller number of rows than split_size value than assign as a split size dataFrame number of rows

        Parameters
        ----------
        dataFrame : pd.DataFrame
            data frame with observations

        Returns
        ------
        list
            list of splits
        """

        #If dataFrame has has smaller number of rows than split_size value than assign as a split size dataFrame number of rows
        if(len(dataFrame) < self.split_size):
            splitted = [[0, len(dataFrame)]]
        else:
            splitted = np.array_split(list(range(0,len(dataFrame))), len(dataFrame)/self.split_size)
            splitted = [[elements[0] - 1, elements[-1]] if elements[0] != 0 else [elements[0], elements[-1]]  for elements in splitted]
            splitted[len(splitted) - 1][1] += 1

        return splitted

    #Insert active links to database
    def insert_active_links(self, dataFrame, column_names):
        """Insert active links to database

        if column names are correct insert observations to table

        Parameters
        ----------
        dataFrame : pd.DataFrame
            data frame with observations
        column_names : list
            column names of table in which you want to insert new observations (without index column)

        Returns
        ------
        str
            information that names are incorrect
        """
        #Create splits to insert big dataset
        splitted = self.create_split(dataFrame, self.split_size)

        #Verify corectness of column names
        if sum(dataFrame.columns == column_names) == len(column_names):
            for split in splitted:
                conn = self.engine.connect()

                #Add observations
                for index, row in dataFrame[split[0]:split[1]].iterrows():
                    conn.execute("INSERT INTO "+self.table_name_links+"\
                                 ([pageName],[link])\
                                 Values (?, ?)\
                                 ",(row["pageName"], row["link"]))
                conn.close()
        else:
            return "Add correct column names"

    #Find new links to scrape and inactive to remove
    def find_links_to_scrape(self, activeLinks, page_name):
        """Find new links to scrape and inactive to remove

        Parameters
        ----------
        activeLinks : list
            list of scraped links (offers are available at webpage)
        page_name : str
            name of the website from which data were scraped

        Returns
        ------
        list
            links that have to be scraped
        list
            links that are no longer available at webpage
        """

        #Select active links from database
        conn = self.engine.connect()
        links_database = pd.DataFrame.from_records(conn.execute("SELECT * FROM "+self.table_name_links+" WHERE [pageName] LIKE '"+page_name+"'").fetchall())

        #Find links to scrape and remove
        activeLinks = pd.DataFrame({"link": activeLinks})
        to_scrape = activeLinks[~activeLinks.stack().isin(links_database.iloc[:,2]).unstack()].dropna()
        to_remove = links_database.iloc[:,2][~links_database.iloc[:,2].isin(activeLinks["link"])].dropna()
        conn.close()

        return to_scrape, to_remove

    #Add new links to database and remove inactive
    def replace_links(self, newLinks, removeLinks, page_name, insert_columns):
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

        """

        #Delete links
        conn = self.engine.connect()
        queries_delete = "DELETE FROM "+self.table_name_links+" WHERE [link] = '"+removeLinks+"'"

        for query in queries_delete:
            conn.execute(query)

        #Insert links
        newLinks = pd.DataFrame({"pageName": page_name, "link": newLinks["link"]})
        self.insert_active_links(dataFrame = newLinks, column_names = insert_columns)

        conn.close()

    def replace_offers(self, removeLinks):

        #Change value for inavtive links
        conn = self.engine.connect()
        queries_delete = "UPDATE " + self.table_name_offers + "SET [active] = 'No' WHERE [link] = '" + removeLinks + "'"

        for query in queries_delete:
            conn.execute(query)

        conn.close()

    def insert_offers(self, offers, insert_columns):

        # Insert new offers to database
        conn = self.engine.connect()

        # Create splits to insert big dataset
        splitted = self.create_split(offers)

        # Verify corectness of column names
        if sum(offers.columns == insert_columns) == len(insert_columns):
            for split in splitted:
                conn = self.engine.connect()

                # Add observations
                for index, row in offers[split[0]:split[1]].iterrows():
                    print(row)
                    conn.execute("INSERT INTO " + self.table_name_offers + "\
                                         ([area],[description],[latitude],[longitude],[link],[price],[currency], [rooms],"
                                                                          "[floors_number],[floor],[type_building],[material_building],"
                                                                          "[year],[headers],[additional_info],[city],[address],"
                                                                          "[district],[voivodeship],[active], [scrape_date],"
                                                                          "[inactive_date], [pageName])\
                                         Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\
                                         ", (row["area"], row["description"], row["latitude"], row["longitude"],
                                             row["link"], row["price"], row["currency"], row["rooms"], row["floors_number"],
                                             row["floor"],row["type_building"], row["material_building"], row["year"],
                                             row["headers"], row["additional_info"], row["city"], row["address"],
                                             row["district"], row["voivodeship"], row["active"], row["scrape_date"],
                                             row["inactive_date"], row["pageName"]))
                conn.close()
        else:
            return "Add correct column names"

    #Activate functions to replace and remove observations
    def push_to_database_links(self, activeLinks, page_name, insert_columns):
        """Activate functions to replace and remove observations

        Parameters
        ----------
        activeLinks : list
            list of scraped links (offers are available at webpage)
        page_name : str
            name of the website from which data were scraped
        insert_columns : list
            column names of table in which you want to insert new observations (without index column)

        """

        #Find which links has to be scraped and which to removed
        scrape, remove = self.find_links_to_scrape(activeLinks = activeLinks, page_name = page_name, split_size = self.split_size)

        #Delete and replace links
        self.replace_links(newLinks = scrape, removeLinks = remove, page_name = page_name, insert_columns = insert_columns, split_size = self.split_size)

        #Update table with offers
        self.replace_offers(removeLinks = remove)

        return scrape

    def push_to_database_offers(self, offers, insert_columns):

        self.insert_offers(offers = offers, insert_columns = insert_columns)