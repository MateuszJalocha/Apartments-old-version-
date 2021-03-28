import pandas as pd
import numpy as np
import re

from google.colab import files
uploaded = files.upload()

path = 'C:/Users/Esterka/Desktop/mieszkania/Dane/'
apartments1 = pd.read_csv('mieszkania506.csv',
                   index_col=0)

apartments2 = pd.read_csv('mieszkania1520.csv',
                    index_col=0)

apartments3 = pd.read_csv('mieszkania2534.csv',
                    index_col=0)

apartments = pd.concat([apartments1, apartments2, apartments3],ignore_index=True, axis=0)

def remove_quotation_marks(information_types,apartment_details):
    apartment_details_copy = apartment_details.copy()
    for information_type in information_types:
        for index in range(len(apartment_details)):
            try:
                apartment_details.loc[:,information_type][index]=apartment_details.loc[:,information_type][index].replace("'","").strip('][').split(', ')
            except:
                apartment_details.loc[:,information_type][index]=apartment_details.loc[:,information_type][index]
    return apartment_details


def numeric_informations(information_types,apartment_details):
    apartment_details_copy = apartment_details.copy()
    for position,information_type in enumerate(information_types):
        try:  
            for index in range(len(apartment_details)):
                apartment_details.loc[:,information_type][index]=float(apartment_details.loc[:,information_type][index][0].replace(",","."))
        except:
            if(information_type!=information_types[-1]):
                information_type=information_types[position+1]
    return apartment_details

def get_string(information_types,apartment_details):
    apartment_details_copy = apartment_details.copy()
    for position,information_type in enumerate(information_types):
        try:
            for index in range(len(apartment_details)):
                if(type(apartment_details.loc[:,information_type][index])!= float):
                    apartment_details.loc[:,information_type][index]=' '.join(map(str, apartment_details.loc[:,information_type][index]))
                else:
                    apartment_details.loc[:,information_type][index]=apartment_details.loc[:,information_type][index]
        except:
            if(information_type!=information_types[-1]):
                information_type=information_types[position+1]
    return apartment_details
   
def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext.split("\n")

def prepare_tables_informations(tables):
    prepared_tables = []
    for table in tables:
        list_info = [information.strip() for information in cleanhtml(table) if information.strip()]
        to_append = dict([x for x in zip(*[iter(list_info)]*2)])
        prepared_tables.append(to_append)
    
    return prepared_tables     

def prepare_additionalParams_information(additional_params, headers):
    prepared_infos = []
    headers = headers[-len(prepared_infos):]
    for i in range(len(additional_params)):
        param = additional_params[i]
        h3 = headers[i]
        to_append = [information.strip() for information in cleanhtml(param) if information.strip()]
        to_append_header = [header.strip() + ":" for header in cleanhtml(h3) if header.strip()]
        prepared_infos.extend([to_append_header, to_append])
        
    return prepared_infos



remove_quotation_marks(apartments.columns, apartments)
numeric_informations(apartments.columns,apartments)
get_string(apartments.select_dtypes(exclude=["float"]).columns, apartments)
apartments['params_tables']=prepare_tables_informations(tables=apartments['params_tables'])
params = prepare_additionalParams_information(additional_params=apartments['params_p'],headers=apartments['params_h3'])
apartments['params_h3'] = params[::2]
apartments['params_p'] = params[1::2]

#################### DODATKOWE - PRÓBY ###################
#####
miasto = []
ulica = []
dodatkowe = []
for index, value in enumerate(apartments.title):
  if "Gdański, Gdynia" in value:
    print(value)
    print(index)
  miasto.append(value.split(", ")[0])
  dodatkowe.append(value.split(", ")[1])
  ulica.append(value.split(", ")[-1])
  
  #####
params_table = pd.DataFrame()
for i in range(len(apartments)):
  column = []
  row = []
  for key, value in apartments.params_tables[i].items():
    column.append(key.strip(':'))
    row.append(value)
  df_temp = pd.DataFrame([row], columns=column)
  params_table = pd.concat([params_table,df_temp])
  
  
  from nltk import tokenize
  tokenize.sent_tokenize(apartments.description[0])[0].split(", ")[0]
  re.search('Księży. Młyn..',tokenize.sent_tokenize(apartments.description[0])[0].split(", ")[0])
