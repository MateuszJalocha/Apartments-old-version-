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
class Preprocessing_Otodom:
    def __init__(self,apartment_details,information_types):
        self.apartment_details = apartment_details
        self.information_types = information_types

    def remove_quotation_marks(self):
        information_types = self.information_types
        apartment_details = self.apartment_details
        for information_type in information_types:
            for index in range(len(apartment_details)):
                try:
                    apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][
                        index].replace("'", "").strip('][')
                except:
                    apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][index]
        return apartment_details


    def numeric_information(self):
        information_types = self.information_types
        apartment_details = self.remove_quotation_marks()
        for position,information_type in enumerate(information_types):
            try:
                for index in range(len(apartment_details)):
                    apartment_details.loc[:,information_type][index]=float(apartment_details.loc[:,information_type][index])
            except:
                if information_type!=information_types[-1]:
                    information_type=information_types[position+1]
        return apartment_details


    def remove_new_line_marks(self):
        information_types = self.information_types
        apartment_details = self.numeric_information()
        for information_type in information_types:
            for index in range(len(apartment_details)):
                try:
                    apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][
                        index].replace('\\n\\n', ', ').replace(', ,', ',')
                except:
                    apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][index]
        return apartment_details

    def prepare_table_information(self,table):
        table = self.remove_new_line_marks()["details"]
        prepared_tables = []
        for row in table:
            list_info = row.replace(":",", ").split(", ")
            to_append = dict([x for x in zip(*[iter(list_info)]*2)])
            prepared_tables.append(to_append)
        return prepared_tables

    def table_information(self, table):
        params_table = pd.DataFrame()
        for i in range(len(table)):
            column = []
            row = []
            for key, value in table[i].items():
                column.append(key.strip(':'))
                row.append(value)
            df_temp = pd.DataFrame([row], columns=column)
            params_table = pd.concat([params_table,df_temp], ignore_index=True)
        return params_table

    def extract_price(self,apartment_details_price_table):
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

    def prepare_additional_info(self,apartment_details_add_info_table, apartment_details_details_table):
        for i in range(len(apartment_details_add_info_table)):
            apartment_details_add_info_table[i]+= apartment_details_details_table[i]
        return apartment_details_add_info_table

    def create_table(self):
        otodom_table = pd.DataFrame()
        params_tables_otodom = self.table_information(self.prepare_table_information(table=self.remove_new_line_marks()["details"]))
        otodom_table["area"] = self.apartment_details.Area
        otodom_table["price"] = self.extract_price(self.remove_new_line_marks()['price'])
        otodom_table["rooms"] = self.apartment_details.Rooms_num
        otodom_table["floor"] = params_tables_otodom.Piętro
        otodom_table["floors_all"] = params_tables_otodom["Liczba pięter"]
        otodom_table["building_type"] = params_tables_otodom["Rodzaj zabudowy"]
        otodom_table["building_material"] = params_tables_otodom["Materiał budynku"]
        otodom_table["building_year"] = params_tables_otodom["Rok budowy"]

        #otodom_table["city"] = params_tables_otodom["Rok budowy"]
        #otodom_table["address"] = params_tables_otodom["Rok budowy"]

        otodom_table["headers"] = self.apartment_details.additional_info_headers
        otodom_table["additional_info"] = self.prepare_additional_info(apartment_details_add_info_table=self.apartment_details.additional_info, apartment_details_details_table = self.apartment_details.details)
        otodom_table["description"] = self.apartment_details.description
        otodom_table["lat"] = self.apartment_details.lat
        otodom_table["lng"] = self.apartment_details.lng
        otodom_table["link"] = self.apartment_details.link
        return otodom_table


if "__name__" == "__main__":
    otodom_preprocess = Preprocessing_Otodom(apartment_details=apartments, information_types=apartments.columns)

    start_time = datetime.now()
    otodom_table = otodom_preprocess.create_table()
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
