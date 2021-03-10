# For more information on using the Census API see:
# U.S. Census Bureau, 
# Using the Census Data API With the 
# American Community Survey,  
# U.S. Government Printing Office, 
# Washington, DC, 2020

#########################################################################################
#########################################################################################

import os
import requests
import json
import pandas as pd
from functools import reduce

class query:
    def __init__(self, year=None, period=None, table=None, 
                 get_acs=None, for_acs=None, in_acs=None, 
                 api_key=os.environ.get('ACS_API_KEY')):
        # base ACS API url
        self.year     = year   # ACS year
        self.period   = period # ACS period (ex: 'acs5' or 'acs1')
        self.table    = table  # ACS table (ex: 'profile' or 'subject')
        self.acs_url  = None   # base ACS api url, generated from year, period, and table

        # ACS API parameters
        self.get_acs = get_acs # Census API get param
        self.for_acs = for_acs # Census API for param
        self.in_acs  = in_acs  # Census API in param
        self.api_key = api_key # Census API key param

        # request results
        self.acs_head = None # used to generate URL for requests.get
        self.acs_df   = None # dataframe to save as JSON
        self.metadata_df = pd.DataFrame(columns=['var', 'label', 'dtype', 'concept'])
 
    def set_acs_url(self): 
        """ Generate base URL for ACS API call
        """
        base_list = [self.year, 'acs', self.period, self.table]
        base_list = [item for item in base_list if item]
        self.acs_url = 'https://api.census.gov/data/' + '/'.join(base_list)
          
    def set_acs_head(self):
        """ Send HEAD request to ACS API.

            This is an inexpensive request that will generate a URL for the GET call.
        """
        if self.acs_url is None: self.set_acs_url()
        payload = {
            'get': self.get_acs,
            'for': self.for_acs,
            'in':  self.in_acs,
            'key': self.api_key}
        self.acs_head = requests.head(self.acs_url, params=payload)

    def set_acs_df(self): 
        """ Save GET call as pandas.DataFrame.
        """
        if self.acs_head is None: self.set_acs_head()
        r = requests.get(self.acs_head.url)
        self.acs_df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
    
    def set_metadata_df(self):
        """ Get descriptive variale names and conceptss for ACS variables.

        Returns:
            pd.DataFrame with colums variable, label, dtype, concept
        """
        if self.acs_url is None: self.set_acs_url()
        vars_raw = requests.get(self.acs_url + '/variables.json')
        vars_raw = vars_raw.json()['variables']
        for var in vars_raw:
            var_dict = vars_raw[var]
            # not all variables have predicateType
            try:
                new_row = [[var, var_dict['label'], var_dict['predicateType'],
                                        var_dict['concept']]]
                self.metadata_df = self.metadata_df.append(
                    pd.DataFrame(new_row, columns=self.metadata_df.columns), 
                    ignore_index=True)
            except KeyError:
                pass

    def select_acs_pe(self):
        """ Select float percentage ACS variables

            Select only percentage columns missing no more than 10%.
            ACS variables ending with PE include mostly percentages, but
            also some aggregate total values.
            I tried using metadata_df and selecting only float columns
            but many of the aggregate total variables are tagged as float.
        """
        # keep only percentage and fips columns
        # percentage fields end with 'PE'
        # GEO_ID contains county fips codes
        if self.acs_df is None: self.set_acs_df()
        self.acs_df = self.acs_df.filter(regex='GEO_ID|PE$')
        pe_vars = self.acs_df.columns.difference(['GEO_ID'])
        self.acs_df[pe_vars] = self.acs_df[pe_vars].apply(pd.to_numeric)
        # replace values not between 0-100 with None
        self.acs_df[pe_vars] = self.acs_df[pe_vars].applymap(
            lambda x: x if x>= 0 and x <= 100 else None)
        # keep columns with no more than 10% missing values. 
        # this should remove all columns that are not percentages
        # and reduce the ammount of error introduced through imputation
        # this method is a little hacky, but avoids having to study all acs variables
        self.acs_df.dropna(axis=1, thresh=len(self.acs_df)/10, inplace=True)

def merge_acs_df(acs_df_list=None):
    """ Merge American Community Survey API query dataframes.

        Args:
            acs_df_list (list of pandas.DataFrame) dataframes to merge
        Returns:
            pandas.DataFrame merged on shared column names
    """
    return reduce(lambda x, y: pd.merge(x, y), acs_df_list)