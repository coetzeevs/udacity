from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


class CustomPlugin(AirflowPlugin):
    """
    Plugin class object to register custom plugins in Airflow environment
    """
    name = "custom_plugin"
    operators = [
        operators.DataQualityOperator,
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
        operators.StageToRedshiftOperator
    ]
    helpers = [
        helpers.CreateTablesSqlQueries,
        helpers.InsertSqlQueries
    ]
