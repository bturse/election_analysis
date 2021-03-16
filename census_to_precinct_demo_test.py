# Overlap TIGER shape files and NYT precint shapes to estimate precinct level
# census demographics.
# Evaluate estimated demographics against official source for a single county.
# This method assumes all demographics within the census region are evenly distributed.
# As this script shows, it is likely not feasible to use tract level census dmeographic
# data to estimate precinct level demographics.
# I ran also ran thi experiment at the ZCTA and block group levels and had similar results

# NYT precinct results: https://github.com/TheUpshot/presidential-precinct-map-2020


#####################################################################################
##################################     MODULES     ##################################
#####################################################################################
import geopandas as gpd
import pandas as pd
import numpy as np
import query_acs
import query_tiger


#####################################################################################
################################     FUNCTIONS     ##################################
#####################################################################################
def get_precinct_results(fips=None, curl_results=True):
    """ Gather NYT precinct election results
        
        https://github.com/TheUpshot/presidential-precinct-map-2020

        Args:
            fips (str) 2 digit state or 5 digit county FIPS code
            curl_results (bool) whether or not to download results
    """
    if curl_results:
        os.system("curl -O https://int.nyt.com/newsgraphics/elections/map-data/2020/national/precincts-with-results.geojson.gz")
        os.system("gunzip precincts-with-results.geojson.gz")
    precinct_results = gpd.read_file('precincts-with-results.geojson')
    precinct_results.rename(columns={'GEOID':'precinct_GEOID'}, inplace=True)
    if fips:
        precinct_results = precinct_results[
            precinct_results['precinct_GEOID'].str.contains("^"+fips)].reset_index(drop=True)
    return precinct_results

def get_overlap(nyt_results, tiger_shapes):
    """ Find intersections between census and precinct shapes.

        Calculate the proportion of a precinct that is within the
        intersection.

        Args:
            nyt_results (GeoPands.GeoDataFrame) created using get_precinct_results
            tiger_shapes (GeoPands.GeoDataFrame) created using query_tiger.get_tiger_shapes
        
        Returns:
            Geopandas.GeoDataFrame with columns:
                precinct_GEOID - precinct id from NYT election results
                tiger_GEOID - census geography id from TIGER
                x_area - area of overlap between precinct and tiger
                ts_area - area of tiger shape region
                x_per_tiger - x_area / ts_area
    """
    tiger_shapes.geometry = tiger_shapes.geometry.to_crs(nyt_results.geometry.crs)
    overlap = {}
    k = 0
    for i, precinct_shape in enumerate(nyt_results['geometry']):
        for j, tiger_shape in enumerate(tiger_shapes['geometry']):
            # precinct and block group borders are often contiguous.
            # adding a small buffer will create an intersection
            pc_buf = precinct_shape.buffer(0.000001)
            ts_buf = tiger_shape.buffer(0.000001)
            if pc_buf.intersects(ts_buf):
                overlap[k] = {
                    'precinct_GEOID':nyt_results.iloc[i]['precinct_GEOID'],
                    'tiger_GEOID':tiger_shapes.iloc[j]['GEOID'],
                    'x_area':pc_buf.intersection(ts_buf).area,
                    'ts_area':ts_buf.area,
                    'x_per_tiger':pc_buf.intersection(ts_buf).area/ts_buf.area}
                k = k + 1
    return gpd.GeoDataFrame.from_dict(overlap, orient='index')


#####################################################################################
#############################     OVERLAP GEOGRAPHIES     ###########################
#####################################################################################
# county 120001 has demographic data available online:
# https://www.votealachua.com/Voters/Precinct-Voting-1/Precinct-Finder
# and https://www.voterfocus.com/PrecinctFinder/precinctStatistics?county=FL-ALA&precinct=01
# we can spot check overlap allocation estimates
nyt_results = get_precinct_results(fips='12001', curl_results=False)
# We are using TRACT in this case, but some small modifications will allow
# this experiment at other census geographic levels such as ZCTA or block group
tiger_shapes = query_tiger.get_tiger_shapes('TRACT', '12')
overlap = get_overlap(nyt_results, tiger_shapes)


#####################################################################################
##################################     ACS POP     ##################################
#####################################################################################
# select variables for total population, total male/female, total white/black
acs_stats = query_acs.query(year='2019', period='acs5',
                            get_acs='B01003_001E,B01001_002E,B01001_026E,B02001_002E,B02001_003E', 
                            for_acs='tract:*', 
                            in_acs='state:12 county:001').get_acs_df()
acs_stats['tiger_GEOID'] = acs_stats['state'] + acs_stats['county'] + acs_stats['tract']
acs_stats.drop(['state', 'county', 'tract'], axis=1, inplace=True)


#####################################################################################
##############################     Estimate Demos      ##############################
#####################################################################################
precinct_pop = pd.merge(overlap, acs_stats)
precinct_pop['pop_pc_in_ts'] = precinct_pop['x_per_tiger']*pd.to_numeric(precinct_pop['B01003_001E'])
precinct_pop['m_pc_in_ts']   = precinct_pop['x_per_tiger']*pd.to_numeric(precinct_pop['B01001_002E'])
precinct_pop['f_pc_in_ts']   = precinct_pop['x_per_tiger']*pd.to_numeric(precinct_pop['B01001_026E'])
precinct_pop['w_pc_in_ts']   = precinct_pop['x_per_tiger']*pd.to_numeric(precinct_pop['B02001_002E'])
precinct_pop['b_pc_in_ts']   = precinct_pop['x_per_tiger']*pd.to_numeric(precinct_pop['B02001_003E'])

estimated_demos = precinct_pop.groupby('precinct_GEOID').agg(
    pc_pop=pd.NamedAgg(column='pop_pc_in_ts', aggfunc=sum),
    f_pop =pd.NamedAgg(column='f_pc_in_ts', aggfunc=sum),
    m_pop =pd.NamedAgg(column='m_pc_in_ts', aggfunc=sum),
    b_pop =pd.NamedAgg(column='b_pc_in_ts', aggfunc=sum),
    w_pop =pd.NamedAgg(column='w_pc_in_ts', aggfunc=sum)).reset_index()

estimated_demos.to_csv('estimated_demos.csv')

#####################################################################################
##############################     Official Demos      ##############################
#####################################################################################
# for this experiment I am using precinct ddemographics from https://www.votealachua.com/
# as the official demographics.
precincts = estimated_demo['precinct_GEOID'].str[-2:].to_list()
official_demos = pd.DataFrame(columns=['precinct_GEOID', 'pc_pop', 'f_pop', 'm_pop', 'b_pop', 'w_pop'])
for p in precincts:
    try:
        pc_stats = pd.read_html('https://www.voterfocus.com/PrecinctFinder/precinctStatistics?county=FL-ALA&precinct='+p)
        new_row = [[
            '12001-' + p,
            pc_stats[1][pc_stats[1]['Gender'] == 'Grand Total']['Total'].to_list()[0],
            pc_stats[1][pc_stats[1]['Gender'] == 'Female']['Total'].to_list()[0],
            pc_stats[1][pc_stats[1]['Gender'] == 'Male']['Total'].to_list()[0],
            pc_stats[0][pc_stats[0]['Race/Gender'] == 'Black total']['Total'].to_list()[0],
            pc_stats[0][pc_stats[0]['Race/Gender'] == 'White total']['Total'].to_list()[0]]]
    except:
        pass
    official_demos = official_demos.append(pd.DataFrame(new_row, columns=official_demos.columns)).reset_index(drop=True)

official_demos.to_csv('official_demos.csv')
