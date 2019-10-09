import pandas as pd
import datetime
from pyspark.sql.functions import udf, col
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.types import *

def build_spark_session():
    """
    Creates and returns spark session object.
    """
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    return spark


def process_imm_locs(filepath, spark=None):
    """
    Reads in path to location dataframe and processes it.
    Returns a spark dataframe.
    """
    # load in
    locs = pd.read_csv(Path(r"{}".format(filepath)),delimiter='|')
    locs['value'] = locs['value'].apply(lambda x: x.replace("\t",'').strip()) 
    locs.rename({'value':'LOCATION_ID'},axis=1,inplace=True)
    locs['i94prtl'] = locs['i94prtl'].apply(lambda x: x.replace("\t",'')) 
    locs['city'] = locs['i94prtl'].apply(lambda x: x.split(",")[0].strip())
    locs['city'] = locs['city'].str.capitalize()
    locs['state'] = locs['i94prtl'].apply(
        lambda x: x.split(",")[1].strip() if len(x.split(",")) > 1 else None)
    locs.drop(['i94prtl'],axis=1,inplace=True)
    # convert location data to spark dataframe for joining
    if spark:
        loc_schema = StructType([
            StructField("LOCATION_ID", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state",StringType(), True)
        ])
        locs = spark.createDataFrame(locs, loc_schema)
    return locs


def convert_imm_ports(df, spark):
    loc_schema = StructType([
            StructField("LOCATION_ID", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state",StringType(), True)
        ])
    df = spark.createDataFrame(df, loc_schema)
    return df


def process_imm_countries(filepath, spark=None):
    """
    Reads in path to countries dataframe and processes it.
    Returns a spark dataframe.
    """
    i94_countries = pd.read_csv(Path(r"{}".format(filepath)),sep="|")
    i94_countries.rename({'value':'COUNTRY_ID'},axis=1,inplace=True)
    # convert country dataframe to spark for joining
    if spark:
        country_schema = StructType([
            StructField("COUNTRY_ID", IntegerType(), True),
            StructField("i94cntyl", StringType(), True)])
        i94_countries = spark.createDataFrame(i94_countries, country_schema)
    return i94_countries


def convert_imm_countries(df, spark):
    country_schema = StructType([
            StructField("COUNTRY_ID", IntegerType(), True),
            StructField("i94cntyl", StringType(), True)])
    df = spark.createDataFrame(df, country_schema)
    return df


def to_sas_datetime(x):
    """Converts SAS numeric datetime to python datetime"""
    try:
        return pd.to_datetime("1/1/1960") + datetime.timedelta(days=int(x))
    except:
        return None
    
udf_to_sas_datetime = udf(lambda x: to_sas_datetime(x), DateType())



def to_datetime_string(x,rev=False):
    """Converts datetime stored as strings to python datetime"""
    try:
        if rev:
            return pd.to_datetime("{}/{}/{}".format(x[4:6],x[-2:],x[:4]))
        else:
            return pd.to_datetime("{}/{}/{}".format(x[:2],x[2:4],x[-4:]))
    except:
        return None

udf_to_datetime_str = udf(lambda x: to_datetime_string(x), DateType())
udf_to_datetime_str_rev = udf(lambda x: to_datetime_string(x,rev=True), DateType())



def clean_immigration_pipeline(filepath, loc_df, country_df, spark):
    # load in
    df_spark= spark.read.parquet(filepath)
    # convert outside dfs to spark df
    loc_df = convert_imm_ports(loc_df, spark)
    country_df = convert_imm_countries(country_df, spark)
    # drop cols
    df_spark = df_spark.drop('visapost','occup','entdepd','entdepu','insnum')
    # convert the country Id to integer
    int_cols = ['i94res','i94cit','i94yr','i94mon','i94mode',
                'i94bir','count','biryear','arrdate','depdate']
    for col in int_cols:
        df_spark = df_spark.withColumn(col, df_spark[col].cast(IntegerType()))
    # join with location and country dfs   
    # join with location and country dfs
    df_spark = df_spark.join(loc_df, df_spark.i94port == loc_df.LOCATION_ID,'left')
    df_spark = df_spark.drop('LOCATION_ID')
    df_spark = df_spark.withColumnRenamed("city", "port_city")    
    df_spark = df_spark.withColumnRenamed("state", "port_state")   
    # countries
    df_spark = df_spark.join(country_df, df_spark.i94res == country_df.COUNTRY_ID,'left')
    # rename i94cntyl and country id
    df_spark = df_spark.drop('COUNTRY_ID')
    df_spark = df_spark.withColumnRenamed("i94cntyl", "residence_country")
    # citizen country
    df_spark = df_spark.join(country_df, df_spark.i94cit == country_df.COUNTRY_ID,'left')
    # rename i94cntyl and country id
    df_spark = df_spark.drop('COUNTRY_ID')
    rename_dict = {
        "i94cntyl":"citizen_country",
        "i94addr":"state_visited",
        "i94visa":"visa_purpose",
        "i94bir":"age",
        "i94mon":"arrival_month",
        "i94yr":"arrival_year",
        "i94mode":"arrival_mode",
        "i94cit":"citizen_country_id",
        "i94res":"residence_country_id",
        "i94port":"port_id"
    }
    for old,new in rename_dict.items():
        df_spark = df_spark.withColumnRenamed(old, new)
    # clean dates
    df_spark = df_spark.withColumn("arrival_date",udf_to_sas_datetime("arrdate"))
    df_spark = df_spark.withColumn("departure_date",udf_to_sas_datetime("depdate"))
    df_spark = df_spark.withColumn("date_addto",udf_to_datetime_str("dtaddto"))
    df_spark = df_spark.withColumn("date_adfile",udf_to_datetime_str_rev("dtadfile"))
    df_spark = df_spark.drop("arrdate","depdate","dtaddto","dtadfile")
    return df_spark