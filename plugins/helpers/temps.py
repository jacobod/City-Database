import pandas as pd
from pathlib import Path

def clean_coord(x,dir_to_change='S'):
    """
    Parses coordinates stored as strings, modifying value as negative
    based on given dir_to_change
    """
    final_val = None
    if (pd.isnull(x)==False):
        orient = x[-1]
        modifier = 1
        if orient == dir_to_change:
            modifier = -1
    final_val = float(x[:-1])*modifier
    return final_val


def clean_temps_pipeline(filepath, cities):
    """
    Takes in the path to the temperature dataframe and processes it.
    All temperatures after the year 2019 are kept.
    Dataframe is then filtered to include only cities
     that are present in the cities dataframe passed in.
    
    Returns:
    * dataframe
    """
    temps = pd.read_csv(Path(r"{}".format(filepath)))
    # filter out missing average temps
    temps = temps[temps['AverageTemperature'].isnull()==False].copy()
    # convert to datetime
    temps['dt'] = pd.to_datetime(temps['dt'])
    # filter for recent dates
    temps = temps[temps['dt']>pd.to_datetime("1/1/1919")].copy()
    # clean latitude and longitude values
    temps['Latitude'] = temps['Latitude'].apply(
        lambda x: clean_coord(x,dir_to_change='S'))
    temps['Longitude'] = temps['Longitude'].apply(
        lambda x: clean_coord(x,dir_to_change='W'))
    return temps