from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    """
    Connects to Redshift and checks if a given table is populated,
    or has a number of records greater than the given (default 0).
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self='',
                 redshift_conn_id='',
                 queries=[],
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries = queries

    def execute(self, context):
        self.log.info("Making Connections to Redshift..")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # fun queries
        for query in self.queries:
            redshift.run(query)
        self.log.info("All tables successfully created.")