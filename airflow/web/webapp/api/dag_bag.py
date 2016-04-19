from flask import abort, current_app, request
from flask.ext.restful import Resource, fields, marshal_with
from airflow.utils.db import provide_session
from airflow.models import DagBagModel

from .parsers import (
    pagination_parser
)

dag_bag_fields = {
    'id': fields.Integer,
    'name': fields.String(),
    'url': fields.String,
    'branch': fields.String,
    'folder': fields.String(),
    'disabled': fields.Boolean(),
    'description': fields.String(attribute='description'),
}

list_fields = {
    'total': fields.Integer,
    'data': fields.List(fields.Nested(dag_bag_fields))
}


class DagBagApi(Resource):
    @marshal_with(dag_bag_fields)
    @provide_session
    def get(self, bag_id, session):
        bag = session.query(DagBagModel).filter_by(
            id=bag_id, ).first()
        if not bag:
            abort(404)
        return bag;

    @provide_session
    def delete(self, bag_id, session=None):
        bag = session.query(DagBagModel).filter(DagBagModel.id == bag_id).first()
        if bag:
            session.delete(bag)
            session.commit()
            return "ok"
        else:
            abort(400)


class DagBagListApi(Resource):
    @marshal_with(list_fields)
    @provide_session
    def get(self, session=None):
        args = pagination_parser.parse_args()
        page = args.get('page')
        per_page = args.get('size')
        if page < 1 or per_page < 1:
            abort(404)
        qry = session.query(DagBagModel)
        qry = qry.order_by(
            DagBagModel.id.desc()
        ).limit(per_page).offset((page - 1) * per_page)

        runs = qry.all()

        if page == 1 and len(runs) < per_page:
            total = len(runs)
        else:
            total = session.query(DagBagModel).order_by(None).count()

        return {"total": total, "data": runs}

    @provide_session
    def post(self, session=None):
        req = request.get_json()
        dag_bag = DagBagModel(
            name=req.get('name'),
            url=req.get('url'),
            branch=req.get('branch'),
            folder=req.get('folder'),
            disabled=req.get('disabled'),
            description=req.get('description')
        )
        if req.has_key('id'):
            dag_bag.id = req.get('id')
        session.merge(dag_bag)
        session.commit()
        return "ok"
