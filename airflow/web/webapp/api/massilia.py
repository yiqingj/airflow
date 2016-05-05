from flask import abort, current_app, request
from flask.ext.restful import Resource, fields, marshal_with
from airflow.models import Artifact
from airflow.utils.db import provide_session

import boto3
from botocore.utils import fix_s3_host
import os
import re

ACCESS_KEY_ID = 'OSMARCHIVE'
os.environ['AWS_ACCESS_KEY_ID'] = ACCESS_KEY_ID
os.environ[
    'AWS_SECRET_ACCESS_KEY'] = 'NClEaax5xMHBpz9Qxs0Gkf0/Ey3Eykc8Yv3Yq2RrzvOHnoc3e0iF/KVS23POhVYk'
endpoint_url = os.environ.get('AWS_ENDPOINT', 'http://store-004.blobstore.apple.com')

from .parsers import (
    massilia_parser
)

blob_fields = {
    'key': fields.String(attribute='Key'),
    'lastModified': fields.DateTime(attribute='LastModified', dt_format='iso8601'),
    'size': fields.String(attribute='Size'),
    'url': fields.String()
}

list_fields = {
    'total': fields.Integer,
    'data': fields.List(fields.Nested(blob_fields))
}


def add_xml_header(params, **kwargs):
    params['headers']['Accept'] = 'application/xml'


class MassiliaListApi(Resource):
    def __init__(self):
        s3 = boto3.resource('s3', endpoint_url=endpoint_url)
        event_system = s3.meta.client.meta.events
        event_system.unregister('before-sign.s3', fix_s3_host)
        event_system.register('before-call.s3.ListObjects', add_xml_header)
        client = boto3.client('s3', endpoint_url=endpoint_url)
        client.meta.events.unregister('before-sign.s3', fix_s3_host)
        client.meta.events.register('before-call.s3.ListObjects', add_xml_header)
        self.s3 = s3
        self.client = client

    @marshal_with(list_fields)
    def get(self):
        args = massilia_parser.parse_args()
        bucket = args.get('bucket')
        prefix = args.get('prefix')

        paginator = self.client.get_paginator('list_objects')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            data = page.get('Contents', [])
            for d in data:
                key = d['Key']
                url = self.client.generate_presigned_url(
                    ClientMethod='get_object',
                    Params={
                        'Bucket': bucket,
                        'Key': key
                    }
                )
                # fix url
                signatures = re.compile(r"Expires=(\d+)&Signature=([a-zA-Z0-9_%]+)")
                m = signatures.search(url)
                if m:
                    fixed_url = '{}/{}/{}?iCloudAccessKeyId={}&Expires={}&Signature={}'.format(
                        endpoint_url,
                        bucket, key, ACCESS_KEY_ID, m.group(1), m.group(2))
                    d['url'] = fixed_url
            return {'total': len(data), 'data': data}
