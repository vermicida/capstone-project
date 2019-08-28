from airflow.contrib.hooks.cassandra_hook import CassandraHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Connection
from airflow.operators import BaseOperator
from airflow.settings import Session
from airflow.utils.decorators import apply_defaults
from capstone_plugin.helpers.cassandra import CassandraQuery
from capstone_plugin.helpers.postgres import PostgresQuery


class CreateTableOperator(BaseOperator):

    """
    This operator handles the creation of the necessary tables in
    the application database.
    """

    ui_color = ''
    ui_fgcolor = ''

    _staging_tables = (
        'weather'
    )

    _prod_tables = (
        'daily_temps_by_city',
        'weekly_temps_by_city',
        'monthly_temps_by_city',
        'quarterly_temps_by_city',
        'yearly_temps_by_city'
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
        Initializes a new instance of the class CreateTableOperator.

        Parameters:
            ddbb_conn_id (str): The identifier of the staging (PostgreSQL)
                or production (Cassandra) database connection.
            table (str): The table you want to create.
        """

        super().__init__(*args, **kwargs)
        self._ddbb_conn_id = ddbb_conn_id
        self._table = table

    def get_connection(self):

        """
        Gets the connection corresponding the given identifier.

        Returns:
            (Connection): The connection object.
        """

        return Session() \
            .query(Connection) \
            .filter(Connection.conn_id == self._ddbb_conn_id) \
            .first()

    def execute(self, context):

        """
        Creates a table using the given database connection.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        conn_type = self.get_connection().conn_type

        if conn_type == 'postgres':
            if self._table not in self._staging_tables:
                raise ValueError('The staging database only supports these tables: {}'.format(', '.join(self._staging_tables)))  # noqa: E501
            query = PostgresQuery().__dir__['{}_creation'.format(self._table)]
            PostgresHook(self._ddbb_conn_id).run(query)

        elif conn_type == 'cassandra':
            if self._table not in self._prod_tables:
                raise ValueError('The production database only supports these tables: {}'.format(', '.join(self._prod_tables)))  # noqa: E501
            query = CassandraQuery().__dir__['{}_creation'.format(self._table)]
            CassandraHook(self._ddbb_conn_id).run(query)

        else:
            raise ValueError('The database connection only supports PostgreSQL and Cassandra types.')  # noqa: E501
