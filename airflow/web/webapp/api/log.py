from flask import abort, current_app, make_response
from flask.ext.restful import Resource, fields, marshal_with
from flask._compat import PY2
from airflow.utils import provide_session, State
from airflow.models import TaskInstance
import os, socket


class LogApi(Resource):
    @provide_session
    def get(self, dag_id, task_id, execution_date, version, session=None):
        ti = session.query(TaskInstance).filter_by(dag_id=dag_id,
                                                   task_id=task_id,
                                                   execution_date=execution_date,
                                                   version=version).first()

        if not ti:
            abort(404)
        log_relative = "{dag_id}/{task_id}/{execution_date}".format(
                **locals())
        loc = os.path.join('/Users/yiqingjin/code_dir/openflow/openflow/logs', log_relative)

        host = ti.hostname
        log_loaded = False
        log = ""
        if socket.gethostname() == host:
            try:
                f = open(loc)
                log += "".join(f.readlines())
                f.close()
                log_loaded = True
            except:
                log = "*** Log file isn't where expected.\n".format(loc)

        else:
            WORKER_LOG_SERVER_PORT = 8080
            url = os.path.join(
                    "http://{host}:{WORKER_LOG_SERVER_PORT}/log", log_relative
            ).format(**locals())
            log += "*** Log file isn't local.\n"
            log += "*** Fetching here: {url}\n".format(**locals())
            try:
                import requests
                log += '\n' + requests.get(url).text
                log_loaded = True
            except:
                log += "*** Failed to fetch log file from worker.\n".format(
                        **locals())

        if PY2 and not isinstance(log, unicode):
            log = log.decode('utf-8')
        resp = make_response(log)
        resp.headers.extend({'Content-Type': 'text/plain'})
        return resp
