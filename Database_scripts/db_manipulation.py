#Database manipulation

#libraries
import pyodbc
import numpy as np
import pandas as pd

#Database connection
server = 'tcp:apartmentsproject.database.windows.net'
database = 'apartments'
username = 'apartments'
password = 'Pingwin9815$'   
driver= '{ODBC Driver 13 for SQL Server}'

databaseConnection = pyodbc.connect('DRIVER='+driver+';\
                    SERVER='+server+';\
                    PORT=1433;\
                    DATABASE='+database+';\
                    UID='+username+';\
                    PWD='+ password)



def insert_active_links(conn, dataFrame, split_size = 1000):
    
    #Create splits to make it possible insert big datasets
    if(len(dataFrame) < split_size):
        splitted = range(0, len(dataFrame))
    else:
        splitted = np.array_split(list(range(0,len(dataFrame))), len(dataFrame)/split_size)
        splitted = [[elements[0] - 1, elements[-1]] if elements[0] != 0 else [elements[0], elements[-1]]  for elements in splitted]
        splitted[len(splitted) - 1][1] += 1
    
    column_names = ["PageName", "Link"]
    
    #Verify corectness of column names
    if sum(dataFrame.columns == column_names) == len(column_names):
        for split in splitted:
            with conn:
                cursor = conn.cursor()
    
                #Add observations
                for index, row in dataFrame[split[0]:split[1]].iterrows():
                    cursor.execute("INSERT INTO Active_links\
                               ([PageName],[Link])\
                               Values (?, ?)\
                               ",(row["PageName"], row["Link"]))
                cursor.close()
    else:
        return "Add correct column names"

def find_link_to_scrape(conn, activeLinks, page_name, split_size = 1000):
    with conn:
        cursor = conn.cursor()
        links_database = cursor.execute("SELECT * FROM Active_links WHERE [PageName] LIKE '"+page_name+"'")
        links_database = pd.DataFrame.from_records([each for each in links_database])
        cursor.close()
       
     activeLinks = pd.DataFrame({"Link": activeLinks})
     to_scrape = activeLinks.iloc[~activeLinks.isin(links_database.iloc[:,2])].dropna()
     
     return to_scrape
 
def replace_links(conn, activeLinks, page_name, split_size = 1000):
    return 0

