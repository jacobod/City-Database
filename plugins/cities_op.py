from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd 
import s3fs
from helpers.cities import create_cities

class ProcessCitiesOperator(BaseOperator):
    """
    General Operator that takes in filepath, the process function, and loads to Redshift.
    The process function generates a temp file in this directory, which is then loaded
    to Redshift. 
    
    """
    ui_color = '#358140'
    # create SQL to format
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
                 *args, **kwargs):

        super(ProcessCitiesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket

    def execute(self, context):

        # initialize the connections
        self.log.info("Making Connections to Redshift..")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Gathering data from Redshift')
    
        # get processed dataframes already in redshift
        us_recs = redshift.get_records("SELECT * FROM us_airports")
        us_airports = pd.DataFrame(us_recs)
        intl_recs = redshift.get_records("SELECT * FROM intl_airports")
        intl_airports = pd.DataFrame(intl_recs)
        dem_recs = redshift.get_records("SELECT * FROM demographics")
        demo_pivot = pd.DataFrame(dem_recs)
        loc_recs = redshift.get_records("SELECT * FROM immigration_ports")
        locs = pd.DataFrame(loc_recs)
        # process
        df = create_cities(us_airports,intl_airports,demo_pivot,locs)

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
            df.to_csv(f)
        # fix temp path so copy-read
        temp_path = "s3n://"+temp_path
        # setting the ignore headers to none if not a csv
        ignore_headers = 'IGNOREHEADER'
        ignore_rows = 1
        load_format = 'csv'
        load_delim = None
        # formatting sql
        formatted_sql = ProcessCitiesOperator.copy_sql.format(
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
