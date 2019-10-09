import pandas as pd
from pathlib import Path

def clean_demos_pipeline(filepath):
    """
    Loads in filepath to demographics file and pivots demographics dataframe. 
    Final DF has each row represent 1 municipality.
    
    Returns:
    * dataframe
    """
    demos = pd.read_csv(Path(r"{}".format(filepath)))
    race_cols = ['American Indian and Alaska Native', 'Asian',
       'Black or African-American', 'Hispanic or Latino', 'White']
    demo_cols = ['Male Population', 'Female Population',
                 'Number of Veterans', 'Foreign-born']
    id_cols = [col for col in demos.columns if col not in ['Race','Count']]
    # pivot
    demo_clean = pd.pivot_table(
        demos, 
        values='Count', 
        index=id_cols, 
        columns=['Race']).reset_index()
    # create percentage columns
    for col in race_cols+demo_cols:
        demo_clean['Percent {}'.format(col)] =\
            demo_clean[col].values/demo_clean['Total Population'].values
        demo_clean.rename(
            {col:'Total {}'.format(col)},axis=1,inplace=True)
    # replace spaces in column names with underscores
    demo_clean.columns = demo_clean.columns.to_series().apply(
        lambda x: x.replace(" ","_").replace("-","_"))
    return demo_clean.reset_index(drop=True)