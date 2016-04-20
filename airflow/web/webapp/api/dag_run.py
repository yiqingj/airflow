import iso8601
import pytz
from flask import abort, current_app, request
from flask.ext.restful import Resource, fields, marshal_with
from airflow.models import DagRun, Task, TaskInstance
from airflow.utils.db import provide_session
from airflow.utils.state import State
from ..errors import DuplicateRecordException
from .parsers import (
    dag_run_parser
)

from .task_instance import task_instance_fields

param_fields = {
    'key': fields.String,
    'value': fields.String
}

dag_run_fields = {
    'id': fields.Integer,
    'dagId': fields.String(attribute='dag_id'),
    'state': fields.String,
    'version': fields.Integer,
    'runId': fields.String(attribute='run_id'),
    'externalTrigger': fields.Boolean(attribute='external_trigger'),
    'executionDate': fields.DateTime(attribute='execution_date', dt_format='iso8601'),
    'startDate': fields.DateTime(attribute='start_date', dt_format='iso8601'),
    'endDate': fields.DateTime(attribute='end_date', dt_format='iso8601'),
    'params': fields.Raw(attribute='conf'),
    'taskRuns': fields.List(fields.Nested(task_instance_fields))
    # 'conf':fields.Arbitrary
}

list_fields = {
    'total': fields.Integer,
    'data': fields.List(fields.Nested(dag_run_fields))
}


class DagRunApi(Resource):
    @marshal_with(dag_run_fields)
    @provide_session
    def get(self, dag_id=None, execution_date=None, session=None):
        run = session.query(DagRun).filter_by(
            dag_id=dag_id,
            execution_date=execution_date).first()
        if not run:
            abort(404)
        task_runs = session.query(TaskInstance).filter(
            TaskInstance.dag_id == run.dag_id,
            TaskInstance.execution_date == run.execution_date).all()
        setattr(run, 'taskRuns', task_runs)
        return run


class DagRunListApi(Resource):
    @marshal_with(list_fields)
    @provide_session
    def get(self, session=None):
        args = dag_run_parser.parse_args()
        page = args.get('page')
        dag_id = args.get('dagId')
        state = args.get('state')

        per_page = args.get('size')
        if page < 1 or per_page < 1:
            abort(404)
        qry = session.query(DagRun)
        if dag_id:
            qry = qry.filter(DagRun.dag_id == dag_id)
        if state:
            qry = qry.filter(DagRun.state == state)
        qry = qry.order_by(
            DagRun.execution_date.desc()
        ).limit(per_page).offset((page - 1) * per_page)

        runs = qry.all()

        for run in runs:
            task_runs = session.query(TaskInstance).filter(
                TaskInstance.dag_id == run.dag_id,
                TaskInstance.execution_date == run.execution_date).all()
            setattr(run, 'taskRuns', task_runs)
        if page == 1 and len(runs) < per_page:
            total = len(runs)
        else:
            total = session.query(DagRun).order_by(None).count()
        return {"total": total, "data": runs}

    @provide_session
    def post(self, session=None):
        req = request.get_json()
        # args = dag_run_post_parser.parse_args()
        run_id = req.get('runId')
        dag_id = req.get('dagId')
        execution_date = req.get('executionDate')
        # execution_date = iso8601.parse_date(execution_date)
        # execution_date = execution_date.astimezone(pytz.utc)

        skipped = [task.get('taskId') for task in req.get('taskRuns') if bool(task.get('skipped'))]

        dr = session.query(DagRun).filter(
            DagRun.dag_id == dag_id, DagRun.run_id == run_id).first()
        if dr:
            raise DuplicateRecordException
        dag_run = DagRun(
            run_id=run_id,
            dag_id=dag_id,
            state=State.RUNNING,
            execution_date=execution_date,
            conf=req.get('params')
        )
        session.add(dag_run)
        tasks = session.query(Task).filter(Task.dag_id == dag_id).all()
        for t in tasks:
            task_run = TaskInstance(
                dag_id=dag_id,
                task_id=t.task_id,
                execution_date=execution_date)
            task_run.upstreams = t.upstreams
            task_run.downstreams = t.downstreams
            task_run.state = State.PENDING
            if t.task_id in skipped:
                task_run.state = State.SKIPPED
            session.add(task_run)
        session.commit()
        return req
