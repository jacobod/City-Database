from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Connects to Redshift and checks if a given table is populated,
    or has a number of records greater than the given (default 0).
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self='',
                 redshift_conn_id='',
                 test_sql='',
                 test_tbl='',
                 expected_results=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_sql = test_sql
        self.test_tbl = test_tbl
        self.expected_results = expected_results

    def execute(self, context):
        self.log.info("Making Connections to Redshift..")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # run the given sql
        n_records = redshift.get_records(self.test_sql)
        n_records = n_records[0]
        # check if matches expected result
        if n_records > self.expected_results:
            raise ValueError(
                """Data Quality check on {} failed with result of {} records.
                 Expected result was greater than {} records""".format(
                    self.test_tbl,n_records,self.expected_results))
        else:
            self.log.info(
                "Data Quality Check on {} passed with {} records".format(
                    self.test_tbl,len(n_records)))