from flask.ext.restful import reqparse, inputs

pagination_parser = reqparse.RequestParser()
pagination_parser.add_argument('page', type=int, default=1)
pagination_parser.add_argument('size', type=int, default=10)
pagination_parser.add_argument('orderBy', default=None)
pagination_parser.add_argument('filter', default=None)
pagination_parser.add_argument('state', default=None)

# dag run


dag_run_parser = pagination_parser.copy()
dag_run_parser.add_argument('dagId')

dag_run_post_parser = reqparse.RequestParser()
dag_run_post_parser.add_argument('dagrun', type=str, location='json')

# end of dag run


task_run_parser = pagination_parser.copy()
task_run_parser.add_argument('dagId')
task_run_parser.add_argument('taskId')
task_run_parser.add_argument('executionDate', type=inputs.datetime_from_iso8601)
task_run_parser.add_argument('version', type=int)

task_run_post_parser = reqparse.RequestParser()
task_run_post_parser.add_argument('taskId')
task_run_post_parser.add_argument('dagId')
task_run_post_parser.add_argument('executionDate', type=inputs.datetime_from_iso8601)
task_run_post_parser.add_argument('mode')  # single, upstream, downstream


event_parser = pagination_parser.copy()
event_parser.add_argument('dagId')
event_parser.add_argument('taskId')
event_parser.add_argument('executionDate', type=inputs.datetime_from_iso8601)
event_parser.add_argument('version', type=int)


artifact_parser = pagination_parser.copy()
artifact_parser.add_argument('dagId')
artifact_parser.add_argument('type')
artifact_parser.add_argument('category')
artifact_parser.add_argument('timestamp')


