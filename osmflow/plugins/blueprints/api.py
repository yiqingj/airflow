from flask import Blueprint, jsonify, request, abort, flash, redirect, url_for
from airflow.utils.db import provide_session
from airflow import configuration
from airflow.models import DagRun, State, Variable, DagBag
from datetime import datetime
from wtforms import StringField, SelectField
from flask_wtf import Form
from flask_wtf import csrf
import logging
import os
import pytz

api = Blueprint(
        "osm_plugin", __name__,
        template_folder='templates',
        static_folder='static',
        static_url_path='/app')


@api.route('/api/adhoc', methods=('GET', 'POST'))
@provide_session
def adhoc_run(session=None):
    print request.form
    dagbag = DagBag(os.path.expanduser(configuration.get('core', 'DAGS_FOLDER')))
    dag = dagbag.get_dag('osm_adhoc')

    class AdHocForm(Form):
        dag_id = SelectField('dag_id',coerce=str, choices=[('debug','debug'),('osm_adhoc','osm_adhoc')])

    user_input_params = {}
    for k, v in dag.params.iteritems():
        setattr(AdHocForm, k, StringField(k))
        user_input_params[k] = None
    form = AdHocForm()

    for k in user_input_params.iterkeys():
        user_input_params[k] = getattr(form, k).data
    execution_date = datetime.utcnow()
    trigger = DagRun(
            dag_id=form.dag_id.data,
            run_id='adhoc_' + execution_date.isoformat(),
            execution_date=execution_date,
            state=State.RUNNING,
            conf=user_input_params,
            external_trigger=True)
    session.add(trigger)
    logging.info("Created {}".format(trigger))
    session.commit()

    flash('Request submitted')
    return redirect(url_for('admin.index'))
