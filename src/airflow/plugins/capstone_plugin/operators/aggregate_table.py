from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from capstone_plugin.helpers import queries


class AggregateTableOperator(BaseOperator):

    """
    This operator aggregates the weather data stored in the staging table and
    pushes it to a given fact table.
    """

    _available_tables = (
        'temps_by_month',
        'temps_by_quarter',
        'temps_by_year'
    )

    @apply_defaults
    def __init__(
        self,
        ddbb_conn_id,
        table,
        *args,
        **kwargs
    ):

        """
        Initializes a new instance of the class AggregateTableOperator.

        Parameters:
            ddbb_conn_id (str): The identifier of the database connection.
            table (str): The table you want to populate.
        """

        super().__init__(*args, **kwargs)
        self._ddbb_conn_id = ddbb_conn_id
        self._table = table

    def execute(self, context):

        """
        Aggregates the weather data stored in the staging table and pushes it
        to the given fact table.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        if self._table not in self._available_tables:
            tables = ', '.join(self._available_tables)
            message = 'Only these tables are supported: {}'.format(tables)
            raise ValueError(message)

        query = queries.INSERTION_QUERIES[self._table]
        PostgresHook(self._ddbb_conn_id).run(query)
