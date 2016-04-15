from flask import abort
from flask.ext.restful import Resource, fields, marshal_with
from airflow.models import Task
from airflow.utils.db import provide_session
from .parsers import (
    pagination_parser,
)

task_fields = {
    'taskId': fields.String(attribute='task_id'),
    'dagId': fields.String(attribute='dag_id'),
    'operator': fields.String(),
    'code': fields.String,
    'upstreams': fields.List(fields.String),
    'downstreams': fields.List(fields.String)
}

class TaskListApi(Resource):
    @marshal_with(task_fields)
    @provide_session
    def get(self, session=None):
        args = pagination_parser.parse_args()
        page = args.get('page')
        per_page = args.get('size')
        filter = args.get('filter')
        category = args.get('category')
        print(args)
        if page < 1 or per_page < 1:
            abort(400)
        query = session.query(Task)
        if filter:
            query = query.filter(Task.task_id.like('%{}%'.format(filter)))
        if category:
            query = query.filter(Task.dag_id == category)
        query = query.limit(per_page).offset((page - 1) * per_page)

        tasks = query.all()
        if page == 1 and len(tasks) < per_page:
            total = len(tasks)
        else:
            total = session.query(Task).order_by(None).count()
        print('page={}, size={}, total={}'.format(page, per_page, total))
        return tasks
