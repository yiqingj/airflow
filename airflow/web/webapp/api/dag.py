from flask import abort, current_app
from flask.ext.restful import Resource, fields, marshal_with
from airflow.models import DagModel, Task
from airflow.utils import provide_session
import json

from .parsers import (
    pagination_parser
)

from .task import task_fields

dag_fields = {
    'dagId': fields.String(attribute='dag_id'),
    'isPaused': fields.Boolean(attribute='is_paused'),
    'isActive': fields.Boolean(attribute='is_active'),
    'schedule': fields.String(),
    'owners': fields.String(),
    'params': fields.Raw,
    'health': fields.String(default='good'),
    'tasks': fields.List(fields.Nested(task_fields))
}

list_fields = {
    'total': fields.Integer,
    'data': fields.List(fields.Nested(dag_fields))
}


class DagApi(Resource):
    @marshal_with(dag_fields)
    @provide_session
    def get(self, dag_id=None, session=None):
        if dag_id:
            dag = session.query(DagModel).filter_by(dag_id=dag_id).first()
            if not dag:
                abort(400)
            # session.expunge(dag)
            tasks = session.query(Task).filter_by(dag_id=dag_id).all()
            setattr(dag, 'tasks', tasks)
            setattr(dag, 'params', json.loads(dag.params))
            return dag

    def post(self):
        pass


class DagListApi(Resource):
    @marshal_with(list_fields)
    @provide_session
    def get(self, session=None):
        args = pagination_parser.parse_args()
        page = args.get('page')

        per_page = args.get('size')
        if page < 1 or per_page < 1:
            abort(400)
        dags = session.query(DagModel).order_by(
                DagModel.last_scheduler_run.desc()
        ).limit(per_page).offset((page - 1) * per_page).all()
        if page == 1 and len(dags) < per_page:
            total = len(dags)
        else:
            total = session.query(DagModel).order_by(None).count()
        for dag in dags:
            session.expunge(dag)
            tasks = session.query(Task).filter_by(dag_id=dag.dag_id).all()
            setattr(dag, 'tasks', tasks)
            setattr(dag, 'params', json.loads(dag.params))
        return {"total": total, "data": dags}

    def post(self):
        pass
