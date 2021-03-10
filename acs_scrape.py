# Generates JSON file from American Communituy Survey API call.
# Select subset of returned variables with regex.
# Useful to query using ACS api groups function and selecting only certain variables
# such as only those ending in PE typically representing percentages.

# For more information on using the Census API see:
# U.S. Census Bureau, 
# Using the Census Data API With the 
# American Community Survey,  
# U.S. Government Printing Office, 
# Washington, DC, 2020

# example:
# > python ./acs_scrape.py --dest 'acs_2019_dp03_state_profile.json'  --year 2019 
# >        --period acs5 --table profile --get_acs 'group(DP03)' 
# >        --for_acs 'state:*' --regex 'GEO_ID|PE$'


#########################################################################################
#########################################################################################

import os
import argparse
import requests
import pandas as pd

def main():
    # process command line arguments
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dest', type=str, 
                            help='path to json file to save census api query results')

    arg_parser.add_argument('--year', type=str, 
                            help='year of census to construct api call')
    arg_parser.add_argument('--period', type=str, 
                            help='period of census to construct api call, ex: "acs5"')
    arg_parser.add_argument('--table', type=str, 
                            help='table of census to construct api call, ex: "profile"')

    arg_parser.add_argument('--get_acs', type=str, default=None, 
                            help='get api parameter value')
    arg_parser.add_argument('--for_acs', type=str, default=None, 
                            help='for api parameter value')
    arg_parser.add_argument('--in_acs' , type=str, default=None, 
                            help= 'in api parameter value')
    arg_parser.add_argument('--api_key', type=str, help='census api key (default: ACS_API_KEY',
                            default=os.environ.get('ACS_API_KEY'))

    arg_parser.add_argument('--items_select', type=str, default=None, 
                            help='pandas filter items parameter to select ACS variables')
    arg_parser.add_argument('--like_select' , type=str, default=None,
                            help='pandas filter like parameter to select ACS variables')
    arg_parser.add_argument('--regex_select', type=str, default=None,
                            help='pandas regex like parameter to select ACS variables')
    arg_parser.add_argument('--axis_select' , type=str, default=None,
                            help='pandas axis like parameter to select ACS variables')

    args = arg_parser.parse_args()

    # Call ACS API
    scrape = acs_scrape()

    scrape.dest    = args.dest
    scrape.year    = args.year
    scrape.period  = args.period
    scrape.table   = args.table
    scrape.get_acs = args.get_acs
    scrape.for_acs = args.for_acs
    scrape.in_acs  = args.in_acs
    scrape.api_key = args.api_key
    scrape.items   = args.items_select
    scrape.like    = args.like_select
    scrape.regex   = args.regex_select
    scrape.axis    = args.axis_select

    scrape.set_acs_df()

    # Save results
    scrape.acs_df.to_json(args.dest)

class acs_scrape:
    def __init__(self):
        self.dest     = None # JSON file to save results

        # base ACS API url
        self.year     = None # ACS year
        self.period   = None # ACS period (ex: 'acs5' or 'acs1')
        self.table    = None # ACS table (ex: 'profile' or 'subject')
        self.acs_url = None # base ACS api url, generated from year, period, and table

        # ACS API parameters
        self.get_acs = None  # Census API get param
        self.for_acs = None  # Census API for param
        self.in_acs  = None  # Census API in param
        self.api_key = None  # Census API key param

        # select subset of ACS variables using dataframe.filter
        self.items = None 
        self.like  = None 
        self.regex = None
        self.axis  = None

        # request results
        self.acs_head = None # used to generate URL for requests.get
        self.acs_df   = None # dataframe to save as JSON
 
    def set_acs_url(self): 
        """ Generate base URL for ACS API call
        """
        base_list = [self.year, 'acs', self.period, self.table]
        base_list = [item for item in base_list if item]
        self.acs_url = 'https://api.census.gov/data/' + '/'.join(base_list)
          
    def set_acs_head(self):
        """ Send HEAD request to ACS API.

            This is an inexpensive request that will generate a URL for the ACS API call.
        """
        if self.acs_url is None: self.set_acs_url()
        payload = {
            'get': self.get_acs,
            'for': self.for_acs,
            'in':  self.in_acs,
            'key': self.api_key}
        self.acs_head = requests.head(self.acs_url, params=payload)

    def set_acs_df(self): 
        """ Save ACS API call as pandas.DataFrame and Select subset of variables.
        """
        if self.acs_head is None: self.set_acs_head()
        r = requests.get(self.acs_head.url)
        self.acs_df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
        try: 
            self.acs_df = self.acs_df.filter(self.items, self.like, self.regex, self.axis)
        except TypeError: 
            pass

if __name__ == '__main__':
  main()
