from airflow.plugins_manager import AirflowPlugin

from capstone_plugin.helpers.postgres import (
    TableCreationQuery,
    TableInsertionQuery
)
from capstone_plugin.hooks.aemet import AemetHook
from capstone_plugin.operators.create_table import CreateTableOperator
from capstone_plugin.operators.import_weather import ImportWeatherOperator


class CapstonePlugin(AirflowPlugin):

    name = 'capstone_plugin'

    helpers = [
        TableCreationQuery,
        TableInsertionQuery
    ]

    hooks = [
        AemetHook
    ]

    operators = [
        CreateTableOperator,
        ImportWeatherOperator
    ]

    sensors = []
    executors = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
