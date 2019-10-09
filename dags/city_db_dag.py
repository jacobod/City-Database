from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from plugins.create_tables_op import CreateTablesOperator
from plugins.helpers.create_tables import create_queries
from plugins.helpers.airports import clean_airports_pipeline
from plugins.process_redshift_op import ProcessToRedshiftOperator
from plugins.helpers.demos import clean_demos_pipeline
from plugins.helpers.temps import clean_temps_pipeline
from plugins.spark_redshift_op import SparksToRedshiftOperator
from plugins.cities_op import ProcessCitiesOperator
from plugins.helpers.immigration import process_imm_countries,process_imm_locs,clean_immigration_pipeline
from plugins.update_city_ids import UpdateCitiesOperator
from plugins.data_quality_op import DataQualityOperator

# TODO
## WRITEUP
## COPY TO WEBSITE TEST ENVIRONMENT

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
aws_credentials_id = 'aws_credentials'
redshift_credentials_id = 'redshift'

# ENTER BUCKET OF FILES HERE
s3_bucket = '' 
# FILES BELOW ARE ASSUMED TO BE IN THE BUCKET
# ENTER FILEPATHS HERE
airports_path = ""
immigration_path = ""
imm_locs_path = ""
imm_countries_path = ""
demographics_path = ""
temps_path = ""


def greeting():
    return "Now creating the cities database in your given redshift cluster."

def goodbye():
    return "Cities database fully set up in redshift."



default_args = {
    'owner': 'jacobdodd',
    'start_date': datetime(2019, 10, 1),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'cities_db_dag',
    default_args=default_args,
    description='Load and transform city data in Redshift with Airflow',
    schedule_interval="@once",
    catchup=False
)


## CREATE OPERATORS
start_process = PythonOperator(
    task_id='Start_setup',  
    dag=dag, 
    provide_context=True,
    python_callable=greeting)

create_tables = CreateTablesOperator(
    task_id='Create_tables', 
    dag=dag,
    provide_context=True,
    redshift_conn_id=aws_credentials_id,
    queries = create_queries)

us_air_operator = ProcessToRedshiftOperator(
    task_id='US_airports', 
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    aws_credentials_id=aws_credentials_id,
    target_table="us_airports",
    process_func=clean_airports_pipeline,
    process_args={"table":"US"},
    s3_bucket=s3_bucket,
    s3_key=airports_path)

intl_air_operator = ProcessToRedshiftOperator(
    task_id='INTL_airports', 
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    aws_credentials_id=aws_credentials_id,
    target_table="intl_airports",
    process_func=clean_airports_pipeline,
    process_args={"table":"INTL"},
    s3_bucket=s3_bucket,
    s3_key=airports_path)


demos_operator = ProcessToRedshiftOperator(
    task_id='Process_demographics', 
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table="demographics",
    aws_credentials_id=aws_credentials_id,
    process_func=clean_demos_pipeline,
    s3_bucket=s3_bucket,
    s3_key=demographics_path)

imm_locs_operator = ProcessToRedshiftOperator(
    task_id='Process_immigration_ports', 
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table="immigration_ports",
    aws_credentials_id=aws_credentials_id,
    process_func=process_imm_locs,
    s3_bucket=s3_bucket,
    s3_key=imm_locs_path)

imm_count_operator = ProcessToRedshiftOperator(
    task_id='Process_immigration_countries', 
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    aws_credentials_id=aws_credentials_id,
    target_table="immigration_countries",
    process_func=process_imm_countries,
    s3_bucket=s3_bucket,
    s3_key=imm_countries_path)


imm_operator = SparksToRedshiftOperator(
    task_id='Process_immigration',  
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    aws_credentials_id=aws_credentials_id,
    target_table="immigration",
    s3_bucket=s3_bucket,
    s3_key=immigration_path
)


cities_operator = ProcessCitiesOperator(
    task_id='Create_cities', 
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    aws_credentials_id=aws_credentials_id,
    target_table="cities",
    s3_bucket=s3_bucket)


temps_operator = ProcessToRedshiftOperator(
    task_id='Process_temps', 
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    aws_credentials_id=aws_credentials_id,
    target_table="city_temps",
    process_func=clean_demos_pipeline,
    s3_bucket=s3_bucket,
    s3_key=demographics_path)


update_intl = UpdateCitiesOperator(
    task_id='Update_intl_ids',  
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table="intl_airports",
    target_col="City_ID",
    join_col="municipality")


update_us = UpdateCitiesOperator(
    task_id='Update_us_ids',  
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table="us_airports",
    target_col="City_ID",
    join_col="municipality")

update_demos = UpdateCitiesOperator(
    task_id='Update_demos_ids',  
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table="demographics",
    target_col="City_ID",
    join_col="City")

check_imm = DataQualityOperator(
    task_id='Data_quality_immigration',  
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    test_sql="SELECT COUNT(*) FROM immigration",
    test_tbl="immigration")


check_cities = DataQualityOperator(
    task_id='Data_quality_cities',  
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    test_sql="SELECT COUNT(*) FROM cities",
    test_tbl="cities")

finalize = PythonOperator(
    task_id="Finalize",
    dag=dag,
    provide_context=True,
    python_callable=goodbye
)

## ORDER operators
start_process >> create_tables

create_tables >> intl_air_operator
create_tables >> us_air_operator
create_tables >> demos_operator
create_tables >> imm_operator
create_tables >> imm_locs_operator
create_tables >> imm_count_operator

intl_air_operator >> cities_operator
us_air_operator >> cities_operator
demos_operator >> cities_operator
imm_operator >> cities_operator
imm_locs_operator >> cities_operator
imm_count_operator >> cities_operator

cities_operator >> temps_operator
cities_operator >> update_demos
cities_operator >> update_intl
cities_operator >> update_us

temps_operator >> check_imm
update_demos >> check_imm
update_intl >> check_imm
update_us >> check_imm
temps_operator >> check_cities
update_demos >> check_cities
update_intl >> check_cities
update_us >> check_cities

check_imm >> finalize
check_cities >> finalize



