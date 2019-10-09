from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class UpdateCitiesOperator(BaseOperator):
    """
    Operator that updates the City_ID column based off of the cities table.
    """
    ui_color = '#358140'
    # create SQL to format
    update_sql = """
        UPDATE {} SET {}=cities.City_ID
        FROM {} t
        JOIN cities ON cities.City=t.{};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 target_col="",
                 join_col="",
                 *args, **kwargs):

        super(UpdateCitiesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.target_col = target_col
        self.join_col = join_col

    def execute(self, context):

        # initialize the connections
        self.log.info("Making Connections to Redshift..")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Updating city id column in table {}'.format(self.target_table))
        # formatting sql
        formatted_sql = UpdateCitiesOperator.update_sql.format(
            self.target_table,
            self.target_col,
            self.target_table,
            self.join_col
        )
        # run load query
        redshift.run(formatted_sql)
