import requests
import geopandas as gpd
import pandas as pd
import numpy as np
import query_acs
from functools import reduce
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
acs_groups=['DP02','DP03','DP04','DP05']
acs_df_list = []
for group in acs_groups:
    acs_query = query_acs.query(year='2019', period='acs5', table='profile',
                                get_acs=group, for_acs='county', in_acs='state:*')

precinct_results = get_precinct_results(curl_results=False)

modeling_df = precinct_results.merge(acs_pe)

X = modeling_df.drop(['PRECINCT_ID', 'fips', 'dem_advantage_pe'], axis=1)
y = modeling_df['dem_advantage_pe']


alphas=(10**np.linspace(-2, 5, 100)).tolist()
pipe = Pipeline([('imputer', KNNImputer()), ('ridge', RidgeCV(alphas))])
pipe.fit(X, y)

# join ridge coefficients and acs metadata to access descriptive variable names
ridge_coef = pd.DataFrame(zip(X.columns, pipe.named_steps.ridge.coef_),
                          columns=['var', 'coef']) 
acs_metadata = get_acs_metadata().filter(['var', 'label', 'concept'])
ridge_coef = ridge_coef.merge(acs_metadata)