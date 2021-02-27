# libraries
import sys
import json
import requests
import pandas as pd
from datetime import datetime
import pandasql as ps

def main():
    ddhq_results = pd.DataFrame(columns=['state_code', 'county_name', 'party_name',
                                         'first_name', 'last_name', 'office',
                                         'incumbent', 'votes'])
    if len(sys.argv) >= 2:
        states = sys.argv[1:]
    else:
        states = get_state_abbr()
    
    for state in states:
        ddhq_state_scrape = ddhq_scrape()
        ddhq_state_scrape.set_ddhq_results_tbl(state)
        ddhq_results = pd.concat([ddhq_results, ddhq_state_scrape.ddhq_results_tbl], 
                                 axis=0)

    dstamp = datetime.today().strftime("%Y%m%d")
    ddhq_results['datestamp'] = dstamp

def get_state_abbr():
    """
    returns list of abbreviations for US states and territories
    """
    states = []
    r = requests.get('https://gist.githubusercontent.com/mshafrir/2646763/raw/8303e1d831e89cb8af24a11f2fa77353c317e408/states_titlecase.json')
    if r.status_code == 200:
        states_json_results = r.json()
        for state in states_json_results:
            abbr = state['abbreviation']
            states.append(abbr)
    return(states)

class ddhq_scrape:
    def __init__(self):
        self.state_json = None
        self.counties_tbl = pd.DataFrame(columns=['ddhq_county_id', 'county_name'])
        self.candidates_tbl = pd.DataFrame(columns=['race_id', 'office', 'state_code', 'cand_id', 
                                                    'party_name', 'first_name', 'last_name', 'incumbent'])
        self.votes_tbl = pd.DataFrame(columns=['race_id', 'cand_id', 'ddhq_county_id', 'votes'])
        self.ddhq_results_tbl = pd.DataFrame(columns=['state_code', 'county_name', 'party_name',
                                                      'first_name', 'last_name', 'office',
                                                      'incumbent', 'votes'])

    def set_state_json(self, state):
        # build url for api query
        state_lower = state.lower()
        state_res_url = 'https://embeds.ddhq.io/api/v2/2020general_results/2020general_'+state_lower
        # attempt to query the api and return json
        try:
            r = requests.get(state_res_url)
        except ValueError:
            print('json not found for: ' + state)
        # if the api query seems successful, return the election results stored under data
        if r.status_code == 200 and isinstance(r.json(), dict):
            self.state_json=r.json()['data']
    
    def set_counties_tbl(self, state):
        if self.state_json == None:
            self.set_state_json(state)
        for race in self.state_json:
            try: 
                county_results = race['countyResults']
                # some state election results are not strictly organized by county.
            except KeyError:
                county_results = race['vcuResults']
            for county in county_results['counties']:
                new_row = [[
                    county['id'],
                    county['county']
                ]]
                self.counties_tbl = self.counties_tbl.append(
                    pd.DataFrame(new_row, columns=self.counties_tbl.columns),
                    ignore_index=True)
                self.counties_tbl = self.counties_tbl.drop_duplicates().reset_index(drop=True)

    def set_candidates_tbl(self, state):
        """
        create table of candidates with candidate and race id, office, state, party, incumbent status
        """
        if self.state_json == None:
            self.set_state_json(state)
        for race in self.state_json:
            candidates = race['candidates']
            for candidate in candidates:
                new_row = [[
                    race['race_id'], 
                    race['office'], 
                    race['abb'],
                    candidate['cand_id'],
                    candidate['party_name'],
                    candidate['first_name'],
                    candidate['last_name'],
                    candidate['incumbent']
                ]]
                self.candidates_tbl = self.candidates_tbl.append(
                    pd.DataFrame(new_row, columns=self.candidates_tbl.columns), 
                    ignore_index=True)

    def set_votes_tbl(self, state):
        if self.state_json == None:
            self.set_state_json(state)
        for race in self.state_json:
            try: 
                counties = race['countyResults']['counties']
            except KeyError:
                counties = race['vcuResults']['counties']
            for county in counties:
                for key in county['votes']:
                    new_row = [[
                        race['race_id'],
                        key,
                        county['id'],
                        county['votes'][key]
                    ]]
                self.votes_tbl = self.votes_tbl.append(
                    pd.DataFrame(new_row, columns=self.votes_tbl.columns), 
                    ignore_index=True)

    def set_ddhq_results_tbl(self, state):
        if self.state_json == None:
            self.set_state_json(state)
        if self.counties_tbl.empty:
            self.set_counties_tbl(state)
        if self.candidates_tbl.empty:
            self.set_candidates_tbl(state)
        if self.votes_tbl.empty:
            self.set_votes_tbl(state)

        # sqldf seems to have trouble accessing tables stored as attributes 
        votes_tbl = self.votes_tbl
        counties_tbl = self.counties_tbl
        candidates_tbl = self.candidates_tbl

        self.ddhq_results_tbl = ps.sqldf("""
            SELECT
              cand.state_code
            , county.county_name
            , cand.party_name
            , cand.first_name
            , cand.last_name
            , cand.office
            , cand.incumbent
            , vote.votes
            FROM votes_tbl vote
            INNER JOIN counties_tbl county ON vote.ddhq_county_id = county.ddhq_county_id
            INNER JOIN candidates_tbl cand ON vote.race_id = cand.race_id AND vote.cand_id = cand.cand_id
            """,
            locals())

if __name__ == '__main__':
  main()
