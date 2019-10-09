from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd 
import os
from pathlib import Path
import time
from pyspark.sql.functions import udf, col
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.types import *
from helpers.immigration import clean_immigration_pipeline
import s3fs

def build_spark_session():
    """
    Creates and returns spark session object.
    """
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    return spark


class SparksToRedshiftOperator(BaseOperator):
    """
    Uses Spark to process data and load it to redshift.
    
    """
    ui_color = '#358140'
    # create SQL to format
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} {}
        '{}' '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table="",
                 s3_bucket='',
                 s3_key="",
                 *args, **kwargs):

        super(SparksToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):

        # initialize the connections
        self.log.info("Making Connections to Redshift..")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Loading the data from {} to Redshift'.format(self.s3_bucket))
        # creating vars to format with
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3n://{}/{}".format(self.s3_bucket, rendered_key)

        # create spark session
        spark = build_spark_session()
        # get the location and country tables already loaded in redshift
        cities_records = redshift.get_records("SELECT * FROM immigration_ports")
        locs = pd.DataFrame(cities_records)
        count_records = redshift.get_records("SELECT * FROM immigration_countries")
        countries_df = pd.DataFrame(count_records)
        # process
        df_spark = clean_immigration_pipeline(s3_path,locs,countries_df,spark)
        # write to s3 using s3fs                
        s3 = s3fs.S3FileSystem(
            anon=False,
            key=credentials.access_key,
            secret=credentials.secret_key)
        # create bucket path
        temp_path = '{}/{}.csv'.format(
            self.s3_bucket,
            self.target_table+"_staging")
        # writing this way ensures no memory errors as streams data
        with s3.open(temp_path,'w') as f:
            df_spark.write.csv(f)
        # fix temp path so copy-read
        temp_path = "s3n://"+temp_path       
        # setting the ignore headers to none if not a csv
        ignore_headers = 'IGNOREHEADER'
        ignore_rows = 1
        load_format = 'csv'
        load_delim = None
        # formatting sql
        formatted_sql = SparksToRedshiftOperator.copy_sql.format(
            self.target_table,
            temp_path,
            credentials.access_key,
            credentials.secret_key,
            ignore_headers,
            ignore_rows,
            load_format,
            load_delim
        )
        # run load query
        redshift.run(formatted_sql)
        # delete tempfile
        os.remove(temp_path)