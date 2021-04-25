import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
from textwrap import wrap
from langdetect import detect

class Preprocessing_Morizon:
    """
        A class used to preprocess offers information from Morizon.pl.
        ...
        Attributes
        ----------
        apartment_details: name of apartments table
        information_types: columns of apartments table
        Methods
        -------
        remove_quotation_marks() -> pd.DataFrame:
            Remove quotation marks from columns.
        numeric_information() -> pd.DataFrame:
            Replace numeric information to float.
        remove_new_line_marks() -> pd.DataFrame:
            Remove new line marks from columns.
        prepare_table_information(table) -> pd.DataFrame:
            Change table information from list to dictionary and create external table from it.
        get_number(price_information: str) -> str:
            Get only numeric information of price from string.
        extract_price(apartment_details_price_table) -> pd.DataFrame:
            Extract price and currency from a string and convert price to float.
        prepare_additional_info(apartment_details_add_info_table, apartment_details_details_table) -> pd.DataFrame:
            Join additional information and details to additional information.
        prepare_description_table(apartment_details_description_table: pd.DataFrame) -> pd.DataFrame:
            Split description of 4000 characters to several columns and if record has more than 16000 characters check the language and get only polish description.
        create_table() -> pd.DataFrame:
            Create final preprocessing table.
        """
    def __init__(self, apartment_details, information_types):
        """
        Parameters
        ----------
        apartment_details : PandasDataFrame
            name of apartments table.
        information_types : str
            columns of apartments table.
        """
        self.apartment_details = apartment_details
        self.information_types = information_types

    def extract_address(self, address_table):
         information_types = self.information_types
         apartment_details = self.apartment_details
         voivodeship = apartment_details.loc[:,address_table].str[2]
         city = apartment_details.loc[:,address_table].str[3]
         district = apartment_details.loc[:,address_table].str[4]
         street = [None if element[4] == element[-1] else element[-1] for element in apartment_details.loc[:,address_table]]

         address = pd.DataFrame({'voivodeship':voivodeship, 'city':city, 'district':district, 'street':street})
         return address
    def extract_currency(self, price_table: pd.DataFrame) -> pd.DataFrame():
         currency =[]
         for index, value in enumerate(price_table):
           try:
             currency.append(value[-1])
           except:
             currency.append(None)
         return currency
    def numeric_information(self) -> pd.DataFrame:
        """Change numeric information to float.
        Parameters
        ----------
        information_types: str
            names of columns from which quotation marks need to be removed
        apartment_details: pd.DataFrame
            data frame with information about apartments.
        Returns
        ------
        apartment_details: pd.DataFrame
            data frame where numeric information type was changed to float.
        """
        information_types = ['price','area']
        apartment_details = self.apartment_details
        for position,information_type in enumerate(information_types):
            try:  
              apartment_details.loc[:,information_type]=apartment_details.loc[:,information_type].apply(lambda x: x[0].replace(",",".").replace(" ","").replace("~","")).astype(np.float)
            except:
              apartment_details.loc[:,information_type]=apartment_details.loc[:,information_type]

        return apartment_details
    
    def remove_quotation_marks(self) -> pd.DataFrame:
        """Remove quotation marks from columns.
         Parameters
         ----------
         information_types: str
             names of columns from which quotation marks need to be removed.
         apartment_details : pd.DataFrame
             data frame with information about apartments.
         Returns
         ------
         apartment_details: pd.DataFrame
             data frame with information about apartments.
         """
        information_types = self.information_types
        apartment_details = self.numeric_information()
        for information_type in information_types:
            for index in range(len(apartment_details)):
                if type(apartment_details.loc[:, information_type][index])==list:
                    apartment_details.loc[:, information_type][index] = list(filter(lambda x: x != "", apartment_details.loc[:, information_type][index]))
                    try:
                        apartment_details.loc[:, information_type][index] = ', '.join(apartment_details.loc[:, information_type][index])
                    except:
                        apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][index]
                else:
                    apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][index]
        return apartment_details


    def remove_new_line_marks(self) -> pd.DataFrame:
        """Remove new line marks from columns.
        Parameters
        ----------
        information_types: str
            names of columns from which new line marks need to be removed.
        apartment_details: pd.DataFrame
            data frame with information about apartments.
        Returns
        ------
        apartment_details: pd.DataFrame
            data frame where new line marks was removed.
        """
        information_types = self.information_types
        apartment_details = self.remove_quotation_marks()
        for information_type in information_types:
            for index in range(len(apartment_details)):
                try:
                    apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][
                        index].replace('\n\n\n\n', ', ').replace("\n",'')
                except:
                    apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][index]
        return apartment_details

    def prepare_table_information(self, table: pd.DataFrame) -> pd.DataFrame:
        """Change table information from list to dictionary and create external table from it.
        Parameters
        ----------
        table: pd.DataFrame
            column with table information.
        Returns
        ------
        prepared_tables: pd.DataFrame
            data frame with table information.
        """
        prepared_table = []
        params_table = pd.DataFrame()
        for index,row in enumerate(table):

          try:
            list_info = row.replace(" ,",", ").replace(":", ", ").replace(" ,","").split(", ")
            without_spaces = [s.strip() for s in list_info]
            to_append = dict([x for x in zip(*[iter(without_spaces)]*2)])
            prepared_table.append(to_append)
          except:
              prepared_table.append(None)
        for i in range(len(prepared_table)):
            column = []
            row = []
            try:
                for key, value in prepared_table[i].items():
                    column.append(key.strip(':'))
                    row.append(value)
            except:
                row.append(None)
            df_temp = pd.DataFrame([row], columns=column)
            params_table = pd.concat([params_table, df_temp], ignore_index=True)

        return params_table.where(pd.notnull(params_table),None)

    def extract_floor(self, floor_table):
          for i in range(len(floor_table)):
            try:
              floor_table[i] = floor_table[i].split()[0]
            except:
              floor_table[i] = None
          return floor_table


    def prepare_description_table(self, apartment_details_description_table: pd.DataFrame) -> pd.DataFrame:
        """Split description of 4000 characters to several columns and if record has more than 16000 characters check the language and get only polish description.
        Parameters
        ----------
        apartment_details_description_table: pd.DataFrame
            column with description.
        Returns
        ------
        description_1: List
            list with tha first part of description.
        """
        description_1 = []
        description_2 = []
        description_3 = []
        description_4 = []


        for i in range(len(apartment_details_description_table)):
          desc_list = [None, None, None, None]
          if len(apartment_details_description_table[i]) > 16000:

            description = apartment_details_description_table[i]
            text = ' '.join(description.replace(",","").replace("-","").split(" ")).split()
            elements = [text[x:x+6] for x in range(0, len(text),6)]

            pl = []
            for index, element in enumerate(elements):
              element = list(map(str.lower,element))
              try:
                language = detect(" ".join(element))
              except:
                language = 'pl'
              if language =='pl':
                pl.append(" ".join(element))

            description_splitted = wrap(" ".join(pl), 4000)

          else:
              try:
                  description_splitted = wrap(apartment_details_description_table[i], 4000)
              except:
                  description_splitted = wrap(''.join(apartment_details_description_table[i]), 4000)


          for element in range(len(description_splitted)):
              desc_list[element] = description_splitted[element]

          description_1.append(desc_list[0])
          description_2.append(desc_list[1])
          description_3.append(desc_list[2])
          description_4.append(desc_list[3])

        self.apartment_details['description_2'] = description_2
        self.apartment_details['description_3'] = description_3
        self.apartment_details['description_4'] = description_4

        return description_1



    def create_table(self):
        """Create final preprocessing table.
        Returns
        ------
        otodom_table: pd.DataFrame
            final preprocessing table with None instead of empty strings.
        """
        morizon_table = pd.DataFrame()
        address = self.extract_address('localization_path')
        currency = self.extract_currency(price_table = self.apartment_details.price)
        params_tables_morizon = self.prepare_table_information(table=self.remove_new_line_marks()['params_tables'])
        #print(params_tables_morizon.columns)
       # nana = self.numeric_information()
        morizon_table["area"] = self.apartment_details.area
        morizon_table["latitude"] = self.apartment_details.lat.astype(np.float)
        morizon_table["longitude"] = self.apartment_details.lng.astype(np.float)
        morizon_table["link"] = self.apartment_details.link
        morizon_table["price"] = self.apartment_details.price
        morizon_table["currency"] = currency
        morizon_table["rooms"] = self.apartment_details.rooms
        morizon_table["floors_number"] = params_tables_morizon["Liczba pięter"]
        morizon_table["floor"] = self.extract_floor(params_tables_morizon['Piętro'])
        morizon_table["type_building"] = params_tables_morizon["Typ budynku"]
        morizon_table["material_building"] = params_tables_morizon["Materiał budowlany"]
        morizon_table["year"] = params_tables_morizon["Rok budowy"]
        morizon_table["headers"] = self.apartment_details.params_h3
        morizon_table["additional_info"] = self.apartment_details.params_p
        morizon_table['city'] = address['city']
        morizon_table['address'] = address['street']
        morizon_table['district'] = address['district']
        morizon_table['voivodeship'] = address['voivodeship']
        morizon_table['active'] = 'Yes'
        morizon_table['scrape_date'] = str(datetime.now().date())
        morizon_table['inactive_date'] = '-'
        morizon_table['pageName'] = 'Morizon'
        morizon_table['description_1'] = self.prepare_description_table(self.apartment_details['description'])
        morizon_table['description_2'] = self.apartment_details['description_2']
        morizon_table['description_3'] = self.apartment_details['description_3']
        morizon_table['description_4'] = self.apartment_details['description_4']
        return morizon_table.replace({"": None})


