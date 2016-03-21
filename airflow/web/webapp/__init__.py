from flask import Flask
from flask.ext.restful import Api
from flask.ext.cors import CORS

from .api.dag_run import DagRunApi, DagRunListApi
from .api.dag import DagApi, DagListApi
from .api.task import TaskListApi
from .api.task_instance import TaskInstanceListApi, TaskInstanceApi

from .api.log import LogApi

# these error messages only works when debug mode is False.
errors = {
    'DuplicateRecordException': {
        'message': "Record already exist in database",
        'status': 400,
    },
    'ResourceDoesNotExist': {
        'message': "A resource with that ID no longer exists.",
        'status': 410,
        'extra': "Any extra information you want.",
    },
}


def create_app(object_name):
    app = Flask(__name__)
    app.config.from_object(object_name)

    rest_api = Api(prefix='/api', errors=errors)
    rest_api.add_resource(DagRunApi, '/dagrun/<dag_id>/<execution_date>')
    rest_api.add_resource(DagRunListApi, '/dagrun')

    rest_api.add_resource(DagListApi, '/dag')
    rest_api.add_resource(DagApi, '/dag/<dag_id>')

    rest_api.add_resource(TaskListApi, '/task')
    rest_api.add_resource(TaskInstanceListApi, '/taskrun')
    rest_api.add_resource(TaskInstanceApi, '/taskrun/<dag_id>/<task_id>/<execution_date>/<version>')

    rest_api.add_resource(LogApi, '/log/<dag_id>/<task_id>/<execution_date>/<version>')

    rest_api.init_app(app)
    CORS(app)
    return app
