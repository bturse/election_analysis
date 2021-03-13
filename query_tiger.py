import pandas as pd
import geopandas as gpd
from datetime import date 
from functools import reduce

# tiget_state_support_dict: whether or not each geographic level support state fips codes
tiget_state_support_dict = {
    'ADDR': True, 'ADDRFEAT': True, 'ADDRFN': True, 'AIANNH': False, 'AITSN': False,
    'ANRC': False, 'AREALM': True, 'AREAWATER': True, 'BG': True, 'CBSA': False, 'CD': True,
    'CNECTA': False, 'COASTLINE': False, 'CONCITY': True, 'COUNTY': True, 'COUSUB': True,
    'CSA': False, 'EDGES': True, 'ELSD': True, 'ESTATE': False, 'FACES': True, 'FACESAH': True,
    'FACESAL': True, 'FACESMIL': False, 'FEATNAMES': True, 'LINEARWATER': True, 'METDIV': False,
    'MIL': False, 'NECTA': False, 'NECTADIV': False, 'PLACE': True, 'POINTLM': True, 
    'PRIMARYROADS': False, 'PRISECROADS': True, 'PUMA': True, 'RAILS': False, 'ROADS': True,
    'SCSD': True, 'SLDL': True, 'SLDU': True, 'STATE': True, 'SUBBARRIO': True, 'TABBLOCK': True,
    'TABBLOCK20': True, 'TBG': False, 'TRACT': True, 'TTRACT': False, 'UAC': False, 'UNSD': True,
    'ZCTA5': False}

def get_tiger_shapes(geography=None, state_fips=None, year=str(date.today().year - 1)):
    """ Get geographic shapes from census.gov

        Args:
            geography (str) Geographic level. Available options here: https://www2.census.gov/geo/tiger/TIGER2020/2020_TL_Shapefiles_File_Name_Definitions.pdf
            state_fips (str) state fips code.
            year (str) year of shape file to return. Default previous year.
        
        Returns:
            pandas.DataFrame with columns GEOID and geometry
        
        Example:
            get_tiger_shapes('CD', '55')
    """
    if not tiget_state_support_dict[geography]:
        raise Exception('geography does not support state_fips')
    tiger_df_list=[]
    geo_tbl_url = 'https://www2.census.gov/geo/tiger/TIGER' + str(year) + '/' + geography.upper() + '/'
    geo_zips = pd.read_html(geo_tbl_url)[0]['Name'].dropna()
    geo_zips = (geo_zips[geo_zips.str.contains('.zip')])
    if state_fips and any(geo_zips.str.contains(str(year)+'_'+state_fips)):
        geo_zips = geo_zips[geo_zips.str.contains('_'+state_fips+'_')].reset_index(drop=True)
    for gz in geo_zips:
        tiger_df = gpd.read_file(geo_tbl_url + gz).filter(regex='GEOID|geometry', axis=1)
        tiger_df.rename(columns={tiger_df.columns[0]: 'GEOID'}, inplace=True)
        tiger_df_list.append(tiger_df)
    tiger_shapes = reduce(lambda x, y: pd.merge(x, y, on='GEOID'), tiger_df_list)
    return(tiger_shapes[tiger_shapes.GEOID.str.contains('^'+state_fips)].reset_index(drop=True))
