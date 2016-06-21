import os

from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow import models

from osmflow.plugins.operators.report_operator import ReportOperator
from osmflow.plugins.operators.misc import JavaOperator, ArtifactoryOperator
from osmflow.plugins.blueprints.api import api

# from osmflow.plugins.views.views import ReportView, AdhocRunView


class OpenFlowPlugin(AirflowPlugin):
    name = "osm_plugin"
    operators = [JavaOperator, ArtifactoryOperator, ReportOperator]
    flask_blueprints = [api]
    # admin_views = [AdhocRunView(name='Adhoc Run')  ]
    executors = []
