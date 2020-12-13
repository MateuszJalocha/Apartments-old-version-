# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 11:19:08 2020

@author: PLESSAI1
"""
#%%
import pandas as pd

#import numpy as np
#from sklearn.impute import KNNImputer


#%%
apartments1 = pd.read_csv('mieszkania506.csv',
                   index_col=0)

apartments2 = pd.read_csv('mieszkania1520.csv',
                    index_col=0)

apartments3 = pd.read_csv('mieszkania2534.csv',
                    index_col=0)

apartments = pd.concat([apartments1, apartments2, apartments3],ignore_index=True, axis=0)
#apartmentss.replace(to_replace=None, value=np.nan)

#%%
def remove_quotation_marks(information_types,apartment_details):
    
    for information_type in information_types:
        for index in range(len(apartment_details)):
            try:
                apartment_details[information_type][index]=apartment_details[information_type][index].replace("'","").strip('][').split(', ')
            except:
                apartment_details[information_type][index]=apartment_details[information_type][index]
    return apartment_details


def numeric_informations(information_types,apartment_details):
    
    for position,information_type in enumerate(information_types):
        try:  
            for index in range(len(apartment_details)):
                apartment_details[information_type][index]=float(apartment_details[information_type][index][0].replace(",","."))
        except:
            if(information_type!=information_types[-1]):
                information_type=information_types[position+1]
    return apartment_details

def get_string(information_types,apartment_details):
    
    for position,information_type in enumerate(information_types):
        try:
            for index in range(len(apartment_details)):
                if(type(apartment_details[information_type][index])!= float):
                    apartment_details[information_type][index]=' '.join(map(str, apartment_details[information_type][index]))
                else:
                    apartment_details[information_type][index]=apartment_details[information_type][index]
        except:
            if(information_type!=information_types[-1]):
                information_type=information_types[position+1]
    return apartment_details
        
remove_quotation_marks(apartments.columns, apartments)
numeric_informations(apartments.columns,apartments)
get_string(apartments.select_dtypes(exclude=["float"]).columns, apartments)

            


