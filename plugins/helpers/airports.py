import pandas as pd
from pathlib import Path

def clean_airports_pipeline(filepath,table='US'):
    """
    Processes the airport dataframe given the path to the airports_csv file.
    International and US airports are handled differently so the US table includes states. 
    This is meant also to improve filtering time in the analytics tables - no need to filter on 
    US airports only.
    
    Returns:
    * intl_aiports (dataframe)
    * us_aiports (dataframe)
    """
    # load in dataframe
    airports = pd.read_csv(Path(r"{}".format(filepath)),low_memory=True)
    # drop cols
    airports = airports.drop(['gps_code','iata_code','local_code'],axis=1)
    airports.rename({'ident':'airport_id'},axis=1,inplace=True)
    airports['longitude'] = airports['coordinates'].apply(
        lambda x: float(x.split(",")[0]))
    airports['latitude'] = airports['coordinates'].apply(
        lambda x: float(x.split(",")[-1]))
    airports.drop(['coordinates'],axis=1,inplace=True)
    # filter types
    airports = airports[airports['type'].isin([
        'heliport','closed','seaplane_base','balloonport'
    ])==False]
    # add in continent for north america
    airports['continent'].fillna(
        airports['iso_country'].apply(
            lambda x: 'NA' if x in ['US','PR','CA','MX'] else None))
    # filter for us airports then get state
    us_airports = airports[airports['iso_country']=='US'].copy()
    us_airports['state'] = us_airports['iso_region'].apply(lambda x: x.split("-")[1])
    us_airports.drop(['continent','iso_country','iso_region'],axis=1,inplace=True)
    # get international airports
    intl_airports = airports[airports['iso_country']!='US'].copy()
    intl_airports = intl_airports[intl_airports['iso_country'].isnull()==False].copy()
    if table=='US':
        return us_airports
    return intl_airports