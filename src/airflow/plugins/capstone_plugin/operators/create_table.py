from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from capstone_plugin.helpers.postgres import TableCreationQuery


class CreateTableOperator(BaseOperator):

    """
    This operator handles the creation of the necessary tables in
    the application database.
    """

    ui_color = ''
    ui_fgcolor = ''

    @apply_defaults
    def __init__(
        self,
        ddbb_conn_id,
        table,
        *args,
        **kwargs
    ):

        """
        Initializes a new instance of the class CreateTableOperator.

        Parameters:
            ddbb_conn_id (str): The PostgreSQL database connection identifier.
            table (str): The table you want to create.
        """

        super().__init__(*args, **kwargs)
        self._ddbb_conn_id = ddbb_conn_id
        self._table = table

    def execute(self, context):

        """
        Creates a table using the given database connection.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        if self._table not in ('weather_staging'):
            raise BaseException('The given table is not supported.')

        query = TableCreationQuery.weather_staging \
            if self._table == 'weather_staging' \
            else ''

        PostgresHook(self._ddbb_conn_id).run(query)
