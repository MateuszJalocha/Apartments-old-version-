import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
start_time = datetime.now()


path_Morizon = 'C:/Users/Estera/OneDrive/magisterka/dane_mieszkania/Morizon/'

# Read filenames from the given path
#data_files = os.listdir('C:/Users/Estera/OneDrive/magisterka/dane_mieszkania/Morizon/')


#def load_files(filenames):
 #  for filename in filenames:
 #       yield pd.read_csv(path_Morizon+filename)


#data = pd.concat(load_files(data_files), ignore_index=True,axis=0)
apartments1 = pd.read_csv(path_Morizon+'mieszkania506.csv',
                   index_col=0)

apartments2 = pd.read_csv(path_Morizon+'mieszkania1520.csv',
                    index_col=0)

apartments3 = pd.read_csv(path_Morizon+'mieszkania2534.csv',
                    index_col=0)

apartments_Morizon = pd.concat([apartments1, apartments2, apartments3],ignore_index=True, axis=0)


def remove_quotation_marks(information_types,apartment_details):
    apartment_details_copy = apartment_details.copy()
    for information_type in information_types:
        for index in range(len(apartment_details)):
            try:
                apartment_details.loc[:,information_type][index]=apartment_details.loc[:,information_type][index].replace("'","").strip('][').split(' ')
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

def prepare_tables_information(tables):
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



remove_quotation_marks(apartments_Morizon.columns, apartments_Morizon)
numeric_informations(apartments_Morizon.columns,apartments_Morizon)
get_string(apartments_Morizon.select_dtypes(exclude=["float"]).columns, apartments_Morizon)
apartments_Morizon['params_tables']=prepare_tables_information(tables=apartments_Morizon['params_tables'])
params = prepare_additionalParams_information(additional_params=apartments_Morizon['params_p'],headers=apartments_Morizon['params_h3'])
apartments_Morizon['params_h3'] = params[::2]
apartments_Morizon['params_p'] = params[1::2]

def table_information(information_types, apartment_details):
  params_table = pd.DataFrame()
  for i in range(len(apartments_Morizon)):
    column = []
    row = []
    for key, value in apartments_Morizon.params_tables[i].items():
      column.append(key.strip(':'))
      row.append(value)
    df_temp = pd.DataFrame([row], columns=column)
    params_table = pd.concat([params_table,df_temp], ignore_index=True)
  return params_table

params_tables_morizon = table_information(apartments_Morizon.columns,apartments_Morizon)

def extract_floor(params_tables_morizon):
    for i in range(len(params_tables_morizon)):
        if type(params_tables_morizon.Piętro[i])==float or params_tables_morizon.Piętro[i]=='nan' :
            params_tables_morizon.Piętro[i]=params_tables_morizon.Piętro[i]
        else:
            params_tables_morizon.Piętro[i]=params_tables_morizon.Piętro[i].split()[0]
        if params_tables_morizon.Piętro[i]=='parter':
            params_tables_morizon.Piętro[i]=0
    return params_tables_morizon.Piętro


morizon_table = pd.DataFrame()
morizon_table["area"] = apartments_Morizon.area
morizon_table["price"] = apartments_Morizon.price
morizon_table["rooms"] = apartments_Morizon.rooms
morizon_table["floor"] = extract_floor(params_tables_morizon)
morizon_table["floors_all"] = params_tables_morizon["Liczba pięter"]
morizon_table["building_type"] = params_tables_morizon["Typ budynku"]
morizon_table["building_material"] = params_tables_morizon["Materiał budowlany"]
morizon_table["building_year"] = params_tables_morizon["Rok budowy"]

#morizon_table["city"] = params_tables_otodom["Rok budowy"]
#morizon_table["address"] = params_tables_otodom["Rok budowy"]

morizon_table["headers"] = apartments_Morizon.params_h3
morizon_table["additional_info"] = apartments_Morizon.params_p
morizon_table["description"] = apartments_Morizon.description
morizon_table["lat"] = apartments_Morizon.lat
morizon_table["lng"] = apartments_Morizon.lng
morizon_table["link"] = apartments_Morizon.link

end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))

#preprocessing_table = pd.concat([morizon_table,otodom_table], axis=0)