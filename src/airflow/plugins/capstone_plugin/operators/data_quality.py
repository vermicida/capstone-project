from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from capstone_plugin.helpers import queries


class DataQualityOperator(BaseOperator):

    """
    This operator checks the quality of one or more given tables.
    """

    _available_tables = (
        'weather_staging',
        'temps_by_month',
        'temps_by_quarter',
        'temps_by_year'
    )

    @apply_defaults
    def __init__(
        self,
        ddbb_conn_id,
        tables,
        *args,
        **kwargs
    ):

        """
        Initializes a new instance of the class DataQualityOperator.

        Parameters:
            ddbb_conn_id (str): The identifier of the database connection.
            tables (tuple): The tables on which the quality tests will
                be performed.
        """

        super().__init__(*args, **kwargs)
        self._ddbb_conn_id = ddbb_conn_id
        self._tables = tables

    def execute(self, context):

        """
        Runs the quality tests over the given tables.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        postgres = PostgresHook(self._ddbb_conn_id)

        for table in self._tables:

            if table not in self._available_tables:
                tables = ', '.join(self._available_tables)
                message = 'Only these tables are supported: {}'.format(tables)
                raise ValueError(message)

            query = queries.SELECTION_QUERIES['count'].format(table)
            records = postgres.get_records(query)

            if len(records) == 0 or len(records[0]) == 0 or records[0][0] == 0:
                message = 'The table {} has not passed the data quality check.'.format(table)
                raise ValueError(message)

            message = 'The table {} has passed the data quality check with {} records.'.format(table, records[0][0])
            self.log.info(message)
