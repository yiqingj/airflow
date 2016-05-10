from flask import abort, request
from flask.ext.restful import Resource, fields, marshal_with
from airflow.models import TaskInstance, DagRun, Task, State
from airflow.utils.db import provide_session
from .parsers import (
    pagination_parser,
    task_run_parser,
    task_run_post_parser
)

task_instance_fields = {
    'taskId': fields.String(attribute='task_id'),
    'dagId': fields.String(attribute='dag_id'),
    'executionDate': fields.DateTime(attribute='execution_date', dt_format='iso8601'),
    'startDate': fields.DateTime(attribute='start_date', dt_format='iso8601'),
    'endDate': fields.DateTime(attribute='end_date', dt_format='iso8601'),
    'duration': fields.Float,
    'state': fields.String,
    'tryNumber': fields.Integer(attribute='try_number'),
    'hostname': fields.String,
    'unixname': fields.String,
    'jobId': fields.Integer(attribute='job_id'),
    'pool': fields.String,
    'queue': fields.String,
    'priorityWeight': fields.Integer(attribute='priority_weight'),
    'operator': fields.String,
    'queuedDttm': fields.DateTime(attribute='queued_dttm', dt_format='iso8601'),
    'version': fields.Integer,
    'upstreams': fields.List(fields.String),
    'downstreams': fields.List(fields.String)
}


class TaskInstanceListApi(Resource):
    @marshal_with(task_instance_fields)
    @provide_session
    def get(self, session=None):
        args = task_run_parser.parse_args()
        page = args.get('page')
        per_page = args.get('size')
        filter = args.get('filter')
        category = args.get('category')
        dag_id = args.get('dagId')
        task_id = args.get('taskId')
        execution_date = args.get('executionDate')
        print(args)
        if page < 1 or per_page < 1:
            abort(400)
        query = session.query(TaskInstance)
        if dag_id:
            query = query.filter(TaskInstance.dag_id == dag_id)
        if task_id:
            query = query.filter(TaskInstance.task_id == task_id)
        if execution_date:
            query = query.filter(TaskInstance.execution_date == execution_date)
        if filter:
            query = query.filter(TaskInstance.task_id.like('%{}%'.format(filter)))
        if category:
            query = query.filter(TaskInstance.dag_id == category)
        query = query.limit(per_page).offset((page - 1) * per_page)

        tasks = query.all()
        if page == 1 and len(tasks) < per_page:
            total = len(tasks)
        else:
            total = session.query(TaskInstance).order_by(None).count()
        print('page={}, size={}, total={}'.format(page, per_page, total))
        return tasks

    @provide_session
    def post(self, session=None):
        """
        This request will trigger a task re-run
        :param session:
        :return:
        """
        req = request.get_json()
        # args = task_run_post_parser.parse_args()
        task_id = req.get('taskId')
        dag_id = req.get('dagId')
        execution_date = req.get('executionDate')
        mode = req.get('mode')
        dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id).filter(
            DagRun.execution_date == execution_date).first()
        dag_run.version += 1  # jump dag run version for each task re-run
        dag_run.state = State.RUNNING

        tasks = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id,
                                                   TaskInstance.execution_date == execution_date,
                                                   ~TaskInstance.expired).all()
        task = None
        for t in tasks:
            if t.task_id == task_id:
                task = t

        if not task:
            abort(400)

        scheduled = []  # avoid duplicate
        if mode == 'single':
            self.schedule_task(task, tasks, execution_date, dag_run.version, session,
                               is_single=True, scheduled=scheduled)
        elif mode == 'upstream':
            self.schedule_task(task, tasks, execution_date, dag_run.version, session,
                               is_upstream=True, scheduled=scheduled)
        elif mode == 'downstream':
            self.schedule_task(task, tasks, execution_date, dag_run.version, session,
                               is_upstream=False, scheduled=scheduled)

        for t in tasks:
            if t.state == State.FAILED and t.task_id not in scheduled:
                TI = TaskInstance
                task_run = TI(
                    task_id=t.task_id,
                    dag_id=t.dag_id,
                    execution_date=execution_date)
                task_run.version = dag_run.version
                task_run.upstreams = t.upstreams
                task_run.downstreams = t.downstreams
                task_run.expire_older_versions(session)
                task_run.state = 'skipped'
                session.merge(task_run)
        session.commit()

        return req

    def schedule_task(self, task, tasks, execution_date, version, session, is_upstream=False,
                      is_single=False, scheduled=None):
        TI = TaskInstance
        task_run = TI(
            task_id=task.task_id,
            dag_id=task.dag_id,
            execution_date=execution_date)
        task_run.version = version
        task_run.upstreams = task.upstreams
        task_run.downstreams = task.downstreams
        task_run.expire_older_versions(session)
        task_run.state = State.PENDING
        if scheduled is not None:
            scheduled.append(task.task_id)
        session.merge(task_run)
        if is_single:
            return
        deps = None
        if is_upstream:
            deps = task.upstreams
        else:
            deps = task.downstreams

        for task_id in deps:
            task = [t for t in tasks if t.task_id == task_id][0]
            if not task.task_id in scheduled:
                self.schedule_task(task, tasks, execution_date, version, session, is_upstream,
                                   is_single, scheduled=scheduled)


class TaskInstanceApi(Resource):
    @marshal_with(task_instance_fields)
    @provide_session
    def get(self, task_id=None, dag_id=None, execution_date=None, version=None, session=None):
        task_run = session.query(TaskInstance).filter_by(dag_id=dag_id,
                                                         task_id=task_id,
                                                         execution_date=execution_date,
                                                         version=version).first()
        if not task_run:
            abort(404)
        return task_run

    @marshal_with(task_instance_fields)
    @provide_session
    def delete(self, task_id=None, dag_id=None, execution_date=None, version=None, session=None):
        task_run = session.query(TaskInstance).filter_by(dag_id=dag_id,
                                                         task_id=task_id,
                                                         execution_date=execution_date,
                                                         version=version).first()
        if not task_run:
            abort(404)

        task_run.state = State.SHUTDOWN
        dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id).filter(
            DagRun.execution_date == execution_date).first()
        dag_run.state = State.FAILED
        session.add(task_run)
        session.commit()
        return task_run
