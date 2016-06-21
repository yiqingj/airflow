from airflow.models import BaseOperator, Artifact

from airflow.utils import apply_defaults
from airflow.utils.db import provide_session


class ReportOperator(BaseOperator):
    template_fields = ('payload',)
    ui_color = '#99ccff'

    @apply_defaults
    def __init__(
            self,
            payload,
            xcom_push=False,
            env=None,
            *args, **kwargs):
        super(ReportOperator, self).__init__(*args, **kwargs)

        self.payload = payload

    def execute(self, context):
        if type(self.payload) is list:
            for item in self.payload:
                item['timestamp'] = context['execution_date']
                item['dag_id'] = self.dag_id
        print(self.payload)
        self.save()

    @provide_session
    def save(self, session=None):
        for artifact in self.payload:
            data = Artifact(
                dag_id=artifact['dag_id'],
                timestamp=artifact['timestamp'],
                type=artifact['type'],
                url=artifact['url']
            )
            session.add(data)
        session.commit()
