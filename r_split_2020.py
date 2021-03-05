import os
import requests
from functools import reduce
import pandas as pd
import numpy as np
import itertools
import json
from sklearn.pipeline import Pipeline
from sklearn.impute import KNNImputer
from sklearn.linear_model import RidgeCV


#####################################################################################
#############################     Election Results     ##############################
#####################################################################################
def scrape_ddhq_results(file_name):
    """ scrape county level election results from DDHQ and save to user's machine
        
        Args:
            file_name (string) path to json file to save election results

        Returns:
            0
    """
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.system('python ./ddhq_scrape_county_returns.py --dest ' + file_name)
    return 0

def get_sparse_party_votes(file_name):
    """ Get Republican and Democratic US House and Presidential results

        Does not return rows for party results in counties with no candidate
        from that party.

    Args:
        file_name (str) path to json file of election results generated using
        ddhq_scrape_county_returns.py
    
    Returns:
        pandas.DataFrame with colums: fips, party_name, office, votes.
    """
    df = pd.read_json(file_name, dtype={'fips':'object'})
    df = df[df['party_name'].isin(['Democratic', 'Republican'])]
    df = df[df['office'].isin(['President', 'US House'])]
    df = df.filter(['fips', 'party_name', 'office', 'votes'])
    return df

def fill_party_votes(sparse_party_votes):
    """ Fill NA election results

        Includes a row for all combination of fips, party_name, and office.
        Some counties did not hacve a candidate from one party. Sets votes to
        0 in this case.

    Args:
        sparse_party_votes (pandas.DataFrame) generated using get_sparse_party_votes()

    Returns:
        pandas.DataFrame with columns fips, party_name, office, votes.
    """
    fips_uq = sparse_party_votes.fips.unique()
    party_uq = ['Democratic', 'Republican']
    office_uq = ['President', 'US House']
    df = pd.DataFrame(itertools.product(fips_uq, party_uq, office_uq), 
                      columns=['fips', 'party_name', 'office'])
    df = df.merge(sparse_party_votes, how='left')
    df['votes'].fillna(0, inplace=True)
    return df

def get_party_votes(file_name):
    """ Get Democratic and Republican House and Presidential votes for all counties

    Args:
        file_name (str) path to json file of election results generated using
        ddhq_scrape_county_returns.py

    Returns:
        pandas.DataFrame with columns fips, 

    """
    df = get_sparse_party_votes(file_name)
    df = fill_party_votes(df)
    df = df.pivot_table(values='votes', aggfunc='sum', index='fips',
                        columns=['party_name', 'office'])
    df.columns = [' '.join(col).strip() for col in df.columns.values]
    df = df.reset_index()
    return df

def get_all_county_votes(file_name):
    """ Calculate total votes in each county per office

    Args:
        file_name (str) path to json file of election results generated using
        ddhq_scrape_county_returns.py

    Returns:
        pandas.DataFrame with columns fips, votes President, votes US House
    """
    df = pd.read_json(file_name, dtype={'fips':'object'})
    df = df[df['office'].isin(['President', 'US House'])]
    df = df.filter(['fips', 'party_name', 'office', 'votes'])
    df = df.groupby(['fips','office']).sum().reset_index()
    df = df.pivot(index='fips', columns=['office']).reset_index()
    df.columns = [' '.join(col).strip() for col in df.columns.values]
    # not all counties report election results
    return df.dropna(axis=0)

def get_party_votes_pe(file_name):
    """ Calculate Republican and Democratic US House and Presidential vote
        percentages.

    Args:
        file_name (str) path to json file of election results generated using
        ddhq_scrape_county_returns.py

    Returns:
        pandas.DataFrame with columns fips, and Democratic and Republican, 
        US House and Presidential vote percentage
    """
    party_votes = get_party_votes(file_name)
    all_county_votes = get_all_county_votes(file_name)
    df = party_votes.merge(all_county_votes)
    df['d_pres_pe']  = df['Democratic President'] / df['votes President']
    df['r_pres_pe']  = df['Republican President'] / df['votes President']
    df['d_house_pe'] = df['Democratic US House']  / df['votes US House']
    df['r_house_pe'] = df['Republican US House']  / df['votes US House']
    df = df.filter(['fips', 'd_pres_pe', 'r_pres_pe', 'd_house_pe', 'r_house_pe'])
    # ddhq does not have election results by party for all counties (ie: 12051)
    return df.dropna(axis=0)

def get_r_pe_split(file_name):
    """ Calculate Republican split ticket voting
        
        Calculates the difference between the Republican US House percent vote
        and Republican Presidential percent vote for each county.
    
    Args:
        file_name (str) path to json file of election results generated using
        ddhq_scrape_county_returns.py
    
    Returns:
        pandas.DataFrame with columns for fips and r_pe_split
    """
    df = get_party_votes_pe(file_name)
    df['r_pe_split'] = df['r_house_pe'] - df['r_pres_pe']
    df = df.filter(['fips', 'r_pe_split'])
    return df.dropna(axis=1)

#####################################################################################
#########################     American Community Survey     #########################
#####################################################################################
def get_acs_groups(groups, key):
    """ Get county level data from american community survey

    Args:
        groups       (list of str) acs api groups of fields to select
        key          (string)      census api key
    
    Returns:
        pandas.DataFrame with columns for fips and selected variables
    """
    acs_df_list = []
    for group in groups:
        r = requests.get(
            "https://api.census.gov/data/2019/acs/acs5/profile?get=group(" +
            group +")&for=county&in=state:*&key=" + key)
        df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
        # last 5 chars of GEO_ID are state and county fips codes
        # it might be better to split this into another function
        df['fips'] = df.GEO_ID.str[-5:]
        df = df.drop('GEO_ID', axis=1)
        acs_df_list.append(df)
    return reduce(lambda x, y: pd.merge(x, y), acs_df_list)

def get_acs_metadata():
    """ Get metadata for ACS 5 year profiles variables

        This is useful to get descriptive variable names and group variables
        by concept to interpret regression results.

    Returns:
        pd.DataFrame with colulms variable, label, dtype, concept
    """
    acs_metadata = pd.DataFrame(columns=['var', 'label', 'dtype', 'concept'])
    vars_raw = requests.get(
        'https://api.census.gov/data/2019/acs/acs5/profile/variables.json')
    vars_raw = vars_raw.json()['variables']
    for var in vars_raw:
        var_dict = vars_raw[var]
        # not all variables have predicateType
        try:
            new_row = [[var, var_dict['label'], var_dict['predicateType'],
                                    var_dict['concept']]]
            acs_metadata = acs_metadata.append(
                pd.DataFrame(new_row, columns=acs_metadata.columns), 
                ignore_index=True)
        except KeyError:
            pass
        acs_metadata.append(new_row)
    return acs_metadata

def get_acs_pe(groups, key):
    """ Select float percentage ACS variables
        
        Select only percentage columns missing no more than 10%.
        ACS variables ending with PE include mostly percentages, but
        also some aggregate total values.
        I tried using get_acs_metadata() and selecting only float columns
        but many of the aggregate total variables are tagged as float.

    Args:
        groups       (list of str) acs api groups of fields to select
        key          (string)      census api key
    
    Returns:
        pandas.DataFrame with columns for fips and selected ACS percentage variables
    """
    df = get_acs_groups(groups, key)
    # keep only percentage and fips columns
    # percentage fields end with 'PE'
    # GEO_ID contains county fips codes
    df = df.filter(regex='PE$|fips')
    non_fips = df.columns.difference(['fips'])
    df[non_fips] = df[non_fips].apply(pd.to_numeric)
    # replace values not between 0-100 with None
    df[non_fips] = df[non_fips].applymap(lambda x: x if x>= 0 and x <= 100 else None)
    # keep columns with no more than 10% missing values. 
    # this should remove all columns that are not percentages
    # and reduce the ammount of error introduced through imputation
    # this method is a little hacky, but avoids having to study all acs variables
    return df.dropna(axis=1, thresh=len(df)/10)


#####################################################################################
################################     Regression     #################################
#####################################################################################
def get_modeling_tables(groups=['DP02','DP03','DP04','DP05'],
                        key=os.environ.get('ACS_API_KEY'), 
                        scrape_results=False, file_name=None):
    """ 
    """
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    if scrape_results: scrape_ddhq_results(file_name) 
    if not scrape_results and file_name not in os.listdir():
        print('need election results, set scrape_results=True')
        print()
        sys.exit(1)
    r_pe_split = get_r_pe_split(file_name)
    acs_vars = get_acs_pe(groups, key)
    acs_r_split = r_pe_split.merge(acs_vars).drop('fips', axis=1)
    X = acs_r_split.drop('r_pe_split', axis=1)
    y = acs_r_split['r_pe_split']
    return(X, y)

# impute missing predictor values and fit and tune ridge regression
# ridge regression will return coefficients for all predictors. This will allow us to 
# understand how each variable impacted split ticket voting.
# all predictors are percent variables, so no need to normalize x
X, y = get_modeling_tables(scrape_results=True, file_name='election_results.json')
alphas=(10**np.linspace(5, 3, 100)).tolist()
pipe = Pipeline([('imputer', KNNImputer()), ('ridge', RidgeCV(alphas))])
pipe.fit(X, y)

# join ridge coefficients and acs metadata to access descriptive variable names
ridge_coef = pd.DataFrame(zip(X.columns, pipe.named_steps.ridge.coef_),
                          columns=['var', 'coef']) 
acs_metadata = get_acs_metadata().filter(['var', 'label', 'concept'])
ridge_coef = ridge_coef.merge(acs_metadata)
