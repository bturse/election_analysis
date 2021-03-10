


#####################################################################################
##################################     Modules     ##################################
#####################################################################################

import geopandas as gpd
import pandas as pd
import query_acs
from sklearn.pipeline import Pipeline
from sklearn.impute import KNNImputer
from sklearn.linear_model import RidgeCV


#####################################################################################
###########################     NYT Precinct Results     ############################
#####################################################################################

def get_precinct_results(curl_results=True):
    if curl_results:
        os.system("curl -O https://int.nyt.com/newsgraphics/elections/map-data/2020/national/precincts-with-results.geojson.gz")
        os.system("gunzip precincts-with-results.geojson.gz")

    precinct_results = gpd.read_file('precincts-with-results.geojson')
    precinct_results['fips'] = precinct_results['GEOID'].str[:5]
    precinct_results.rename(columns={'GEOID': 'PRECINCT_ID'}, inplace=True)

    precinct_results['dem_vote_pe'] = precinct_results['votes_dem'] / precinct_results['votes_total']
    precinct_results['rep_vote_pe'] = precinct_results['votes_rep'] / precinct_results['votes_total']
    precinct_results['dem_advantage_pe'] = precinct_results['dem_vote_pe'] = precinct_results['rep_vote_pe']

    return precinct_results.filter(['PRECINCT_ID', 'fips', 'dem_advantage_pe'])


#####################################################################################
#########################     American Community Survey     #########################
#####################################################################################

def get_acs_vars(acs_groups):
    acs_vars_list = []
    for group in acs_groups:
        acs_query = query_acs.query(year='2019', period='acs5', table='profile',
                                    get_acs=group, for_acs='county', in_acs='state:*')
        acs_query.select_acs_pe()
        acs_vars_list.append(acs_query.acs_df)
    acs_vars = query_acs.merge_acs_df(acs_vars_list)
    acs_vars['fips'] = acs_vars.GEO_ID.str[-5:]
    acs_vars.drop('GEO_ID', axis=1, inplace=True)
    return acs_vars


#####################################################################################
################################     Regression     #################################
#####################################################################################

def get_modeling_tables():
    acs_vars = get_acs_vars(
        acs_groups=['group(DP02)','group(DP03)','group(DP04)','group(DP05)'])
    precinct_results = get_precinct_results(curl_results=False)
    modeling_df = precinct_results.merge(acs_vars)
    X = modeling_df.drop(['PRECINCT_ID', 'fips', 'dem_advantage_pe'], axis=1)
    y = modeling_df['dem_advantage_pe']
    return (X, y)

X, y = get_modeling_tables()