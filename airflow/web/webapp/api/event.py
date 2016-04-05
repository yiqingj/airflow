from datetime import datetime
import os
from flask import abort, request
from flask.ext.restful import Resource, fields, marshal_with
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from .parsers import (
    event_parser,
)

event_fields = {
    'taskId': fields.String(attribute='task_id'),
    'dagId': fields.String(attribute='dag_id'),
    'operator': fields.String(),
    'code': fields.String,
    'upstreams': fields.List(fields.String),
    'downstreams': fields.List(fields.String)
}

url_string = os.environ.get('ELASTIC_SERACH_URL','17.135.86.191:9200')
urls = url_string.split(',')

es = Elasticsearch(
    urls
)


class EventListApi(Resource):
    @marshal_with(event_fields)
    def get(self, session=None):
        """ If we use ElasticSearch we might not need a get here.
        """
        pass

    def post(self):
        """
        This request will handle event pushed from pipeline run and store them in ES
        :param session:
        :return:
        """
        req = request.get_json()
        if type(req) != list:
            req = [req]

        actions = []

        for r in req:
            dimensions = {}
            metrics = {}
            now = datetime.utcnow()
            event = {'dimensions': dimensions, 'metrics': metrics,
                     'date': now.isoformat()}
            for d in r['dimensions']:
                dimensions[d['name']] = d['value']
            for m in r['metrics']:
                metrics[m['name']] = m['value']
            action = {
                '_index': 'event-{}'.format(now.strftime('%Y.%m.%d')),
                '_type': 'event',
                '_source': event,
            }
            actions.append(action)
        bulk(es, actions)
        return "ok"
