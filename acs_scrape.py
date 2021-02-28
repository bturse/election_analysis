import os
import argparse
import requests
import pandas as pd

def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dest', type=str)

    arg_parser.add_argument('--get_acs', type=str, default=None)
    arg_parser.add_argument('--for_acs', type=str, default=None)
    arg_parser.add_argument('--in_acs' , type=str, default=None)

    arg_parser.add_argument('--items_select', type=str, default=None)
    arg_parser.add_argument('--like_select' , type=str, default=None)
    arg_parser.add_argument('--regex_select', type=str, default=None)
    arg_parser.add_argument('--axis_select' , type=str, default=None)

    arg_parser.add_argument('--dataset', type=str, default='2019/acs/acs5/profile')
    arg_parser.add_argument('--api_key', type=str, default=os.environ.get('ACS_API_KEY'))

    args = arg_parser.parse_args()

    acs_call = acs_scrape()

    acs_call.get_acs = args.get_acs
    acs_call.for_acs = args.for_acs
    acs_call.in_acs  = args.in_acs

    acs_call.items = args.items_select
    acs_call.like  = args.like_select
    acs_call.regex = args.regex_select
    acs_call.axis  = args.axis_select

    acs_call.dataset = args.dataset
    acs_call.api_key = args.api_key

    acs_call.set_acs_df()

    acs_call.acs_df.to_json(args.dest)

class acs_scrape:
    def __init__(self):
        self.get_acs = None 
        self.for_acs = None 
        self.in_acs  = None 

        self.items = None
        self.like  = None
        self.regex = None
        self.axis  = None

        self.census_api = 'https://api.census.gov/data/'
        self.dataset = None
        self.api_key = None

        self.acs_head = None
        self.acs_df   = None

    def set_acs_head(self):
        payload = {
            'get': self.get_acs,
            'for': self.for_acs,
            'in': self.in_acs,
            'key': self.api_key
        }
        self.acs_head = requests.head(self.census_api + self.dataset, params=payload)

    def set_acs_df(self):
        if self.acs_head is None: self.set_acs_head()
        r = requests.get(self.acs_head.url)
        self.acs_df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
        self.acs_df = self.acs_df.filter(self.items, self.like, self.regex, self.axis)

if __name__ == '__main__':
  main()
