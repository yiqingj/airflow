"""
boto3 looks for AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY environment variables
to do authentication
"""

import boto3
from botocore.utils import fix_s3_host
import os

os.environ['AWS_ACCESS_KEY_ID'] = 'OSMARCHIVE'
os.environ[
    'AWS_SECRET_ACCESS_KEY'] = 'NClEaax5xMHBpz9Qxs0Gkf0/Ey3Eykc8Yv3Yq2RrzvOHnoc3e0iF/KVS23POhVYk'
endpoint_url = 'http://store-001.blobstore.apple.com'


def add_xml_header(params, **kwargs):
    params['headers']['Accept'] = 'application/xml'


class Massilia(object):
    def __init__(self):
        endpoint_url = 'https://store-004.blobstore.apple.com'
        s3 = boto3.resource('s3', endpoint_url=endpoint_url)
        event_system = s3.meta.client.meta.events
        event_system.unregister('before-sign.s3', fix_s3_host)
        event_system.register('before-call.s3.ListObjects', add_xml_header)
        client = boto3.client('s3', endpoint_url=endpoint_url)
        client.meta.events.unregister('before-sign.s3', fix_s3_host)
        client.meta.events.register('before-call.s3.ListObjects', add_xml_header)
        self.s3 = s3
        self.client = client

    def put(self, bucket, key):
        with open('testfile', 'rb') as file_stream:
            self.s3.Object(bucket, key).put(Body=file_stream)

    def list(self, bucket, prefix):
        paginator = self.client.get_paginator('list_objects')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for content in page.get('Contents', []):
                print content['LastModified'].isoformat(), content['Key'], content['Size']

    def get(self, bucket, key):
        obj = self.s3.Object(bucket_name=bucket, key=key)
        print obj.get()['Body'].read()

    def list_buckets(self):
        raise Exception('Not Implemented')


m = Massilia()

m.list('pipeline', '20160503T202321')
m.get('osmtest','test_dir/1')
