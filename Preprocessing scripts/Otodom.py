import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
start_time = datetime.now()

path = 'C:/Users/Estera/OneDrive/magisterka/dane_mieszkania/Otodom/'
# Read filenames from the given path
data_files = os.listdir('C:/Users/Estera/OneDrive/magisterka/dane_mieszkania/Otodom/')


def load_files(filenames):
    for filename in filenames:
        yield pd.read_csv(path+filename)


data = pd.concat(load_files(data_files), ignore_index=True,axis=0)

apartments = data.copy().drop(['Unnamed: 0'],axis=1)
'''
apartments1 = pd.read_csv(path+'mieszkania506.csv',
                   index_col=0)

apartments2 = pd.read_csv(path+'mieszkania1013.csv',
                    index_col=0)

apartments3 = pd.read_csv(path+'mieszkania1520.csv',
                    index_col=0)

apartments = pd.concat([apartments1, apartments2, apartments3],ignore_index=True, axis=0)
'''

def remove_quotation_marks(information_types, apartment_details):
    for information_type in information_types:
        for index in range(len(apartment_details)):
            try:
                apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][
                    index].replace("'", "").strip('][')
            except:
                apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][index]
    return apartment_details


def numeric_information(information_types,apartment_details):
  for position,information_type in enumerate(information_types):
    try:
        for index in range(len(apartment_details)):
            apartment_details.loc[:,information_type][index]=float(apartment_details.loc[:,information_type][index])
    except:
        if(information_type!=information_types[-1]):
            information_type=information_types[position+1]
  return apartment_details


def remove_new_line_marks(information_types, apartment_details):
    for information_type in information_types:
        for index in range(len(apartment_details)):
            try:
                apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][
                    index].replace('\\n\\n', ', ').replace(', ,', ',')
            except:
                apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][index]
    return apartment_details

remove_quotation_marks(apartments.columns,apartments)
numeric_information(apartments.columns,remove_quotation_marks(apartments.columns,apartments))
remove_new_line_marks(apartments.columns,remove_new_line_marks(apartments.columns,apartments))

tables=apartments['details']
def prepare_table_information(tables):
  prepared_tables = []
  for table in tables:
    list_info = table.replace(":",", ").split(", ")
    to_append = dict([x for x in zip(*[iter(list_info)]*2)])
    prepared_tables.append(to_append)
  return prepared_tables

def table_information(information_types, apartment_details):
  params_table = pd.DataFrame()
  information_types = prepare_table_information(apartments['details'])
  for i in range(len(information_types)):
    column = []
    row = []
    for key, value in information_types[i].items():
      column.append(key.strip(':'))
      row.append(value)
    df_temp = pd.DataFrame([row], columns=column)
    params_table = pd.concat([params_table,df_temp], ignore_index=True)
  return params_table

def extract_price(apartment_details_price_table):
    for i in range(len(apartment_details_price_table)):
        if apartment_details_price_table[i]=="" or apartment_details_price_table[i]=="None":
            apartment_details_price_table[i]=""
        elif any(c.isalpha() for c in apartment_details_price_table[i]):
            filtered_str = filter(str.isdigit, apartment_details_price_table[i])
            only_digit = "".join(filtered_str)
            if only_digit == "" or only_digit == "None":
                apartment_details_price_table[i] = ""
            else:
                apartment_details_price_table[i] = float(only_digit.replace(",","."))
        else:
            apartment_details_price_table[i] = float(apartment_details_price_table[i])
    return apartment_details_price_table


prepare_table_information(apartments['details'])
params_tables_otodom = table_information(apartments.columns, apartments)

def prepare_additional_info(apartment_details_add_info_table, apartment_details_details_table):
    for i in range(len(apartment_details_add_info_table)):
        apartment_details_add_info_table[i]+= apartment_details_details_table[i]
    return apartment_details_add_info_table


#create preprocessing table
otodom_table = pd.DataFrame()
otodom_table["area"] = apartments.Area
otodom_table["price"] = extract_price(apartments.price)
otodom_table["rooms"] = apartments.Rooms_num
otodom_table["floor"] = params_tables_otodom.Piętro
otodom_table["floors_all"] = params_tables_otodom["Liczba pięter"]
otodom_table["building_type"] = params_tables_otodom["Rodzaj zabudowy"]
otodom_table["building_material"] = params_tables_otodom["Materiał budynku"]
otodom_table["building_year"] = params_tables_otodom["Rok budowy"]

#otodom_table["city"] = params_tables_otodom["Rok budowy"]
#otodom_table["address"] = params_tables_otodom["Rok budowy"]

otodom_table["headers"] = apartments.additional_info_headers
otodom_table["additional_info"] = prepare_additional_info(apartment_details_add_info_table=apartments.additional_info, apartment_details_details_table = apartments.details)
otodom_table["description"] = apartments.description
otodom_table["lat"] = apartments.lat
otodom_table["lng"] = apartments.lng
otodom_table["link"] = apartments.link
end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))