import pandas as pd
import datetime



def create_cities(us_airports,intl_airports,demo_pivot,locs):
    """
    Creates a composite cities dataset based on the below datasets.
    Datasets needed:
    - intl airports
    - us airports
    - demographics
    - immigration locations
    
    States is a global list generated above.
    """
    # used to fill in missing state values
    states = ['AK', 'AL', 'AR(BPS)', 'AR', 'AZ', 'CA', 'CA(BPS)',
       'CO',  'CT', 'DE', 'FL', 'FL#ARPT', 'GA', 'GU', 'HI',
       'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'LA(BPS)', 'MA', 'WA',
       'MD', 'ME', 'MT', 'ME(BPS)', 'MI', 'MN', 'SPB', 'MO', 'MS',
       'MT(BPS)', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM(BPS)', 'NM', 'NV',
       'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SC#ARPT',
        'SD', 'SPN', 'TN', 'TX', 'TX(BPS)', 
       'VI', 'UT', 'VA', 'VA#ARPT', 'VT', 'VT(I-91)', 'VT(RT.5)',
       'VT(BP-SECTORHQ)', 'WASHINGTON#INTL', 'WA(BPS)', 'WI', 'WV', 'WY',]
    # clean up the individual dataframes
    airport_cities = us_airports\
        [us_airports['municipality'].isnull()==False]\
        [['municipality','state']]\
        .drop_duplicates()\
        .rename({
            "municipality":'City',
            "state":'State'
        },axis=1)\
        .copy()
    airport_cities['Country'] = 'United States'
    
    intl_cities = intl_airports\
        [intl_airports['municipality'].isnull()==False]\
        [['municipality','iso_country']]\
        .drop_duplicates()\
        .rename({
            "municipality":'City',
            "iso_country":'Country'
        },axis=1)\
        .copy()

    demo_cities = demo_pivot[['City','State_Code']]\
        .rename({'State_Code':'State'},axis=1).copy()
    demo_cities['Country'] = 'United States'
    
    loc_cities = locs[locs['state'].isnull()==False]\
        [['city','state']]\
        .drop_duplicates()\
        .rename({'city':'City','state':'State'},axis=1)\
        .copy()
    # add all together
    cities_df = pd.concat(
        [demo_cities,airport_cities],
        ignore_index=True,
        sort=False)
    cities_df = pd.concat(
        [cities_df,intl_cities],
        ignore_index=True,
        sort=False)
    cities_df = pd.concat(
        [cities_df,loc_cities],
        ignore_index=True,
        sort=False)
    cities_df = cities_df.drop_duplicates(keep='first')
    # fill in country
    cities_df['Country'].fillna(
        cities_df['State'].apply(
            lambda x: 'United States' if x in states else None),
        inplace=True)
    cities_df['Country'].fillna(cities_df['State'],inplace=True)
    # drop duplicates
    cities_df = cities_df.drop_duplicates()
    # assign unique ID
    cities_df['City_ID'] = cities_df.reset_index().index+1
    cities_df['City_ID'] = cities_df.apply(
        lambda x: str(x['City_ID'])+x['City'].replace(" ","").upper()+x['State'].upper(),axis=1)
    # add back in the port id
    cities_df = pd.merge(
        left=cities_df,
        right=locs.rename({'city':'City','state':'State'},axis=1),
        on=['City','State'],
        how='left'
    )
    cities_df.rename({'LOCATION_ID':'Port_ID'},axis=1,inplace=True)
    return cities_df
