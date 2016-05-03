from flask import abort, current_app, request
from flask.ext.restful import Resource, fields, marshal_with
from airflow.models import Artifact
from airflow.utils.db import provide_session

from .parsers import (
    artifact_parser
)

param_fields = {
    'key': fields.String,
    'value': fields.String
}

artifact_fields = {
    'id': fields.String(),
    'name': fields.String(),
    'type': fields.String(),
    'category': fields.String(),
    'timestamp': fields.String(),
    'url': fields.String(),
    'tags': fields.Raw()
}

list_fields = {
    'total': fields.Integer,
    'data': fields.List(fields.Nested(artifact_fields))
}


class ArtifactApi(Resource):
    @marshal_with(artifact_fields)
    @provide_session
    def get(self, artifact_id=None, session=None):
        artifact = session.query(Artifact).filter(Artifact.id == artifact_id).first()
        if not artifact:
            abort(404)
        return artifact


class ArtifactListApi(Resource):
    @marshal_with(list_fields)
    @provide_session
    def get(self, session=None):
        args = artifact_parser.parse_args()
        page = args.get('page')
        per_page = args.get('size')
        dag_id = args.get('dagId')
        type = args.get('type')
        category = args.get('category')
        timestamp = args.get('timestamp')
        query = session.query(Artifact)
        if dag_id:
            query = query.filter(Artifact.dag_id == dag_id)
        if type:
            query = query.filter(Artifact.type == type)
        if category:
            query = query.filter(Artifact.category == category)
        if timestamp:
            query = query.filter(Artifact.timestamp == timestamp)

        query = query.limit(per_page).offset((page - 1) * per_page)

        artifacts = query.all()
        if page == 1 and len(artifacts) < per_page:
            total = len(artifacts)
        else:
            total = session.query(Artifact).order_by(None).count()
        return {"total": total, "data": artifacts}

    @provide_session
    def post(self, session=None):
        req = request.get_json()
        artifact = Artifact(
            name=req.get('name'),
            type=req.get('type'),
            category=req.get('category'),
            timestamp=req.get('timestamp'),
            url=req.get('url'),
            tags=req.get('tags'),
        )
        session.add(artifact)
        session.commit()
        return 'ok'
