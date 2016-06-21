from airflow.operators import BashOperator
from airflow.utils import apply_defaults
import os


class JavaOperator(BashOperator):
    @apply_defaults
    def __init__(self,
                 main_class,
                 class_path='.',
                 op_args=None,
                 *args,
                 **kwargs):
        cp = os.environ.get('CLASSPATH', '.')
        class_path = ':'.join([class_path, cp])
        command = 'java -cp {class_path} {main_class}'
        if op_args:
            command += ' ' + ' '.join(op_args)
        command = command.format(**locals())
        super(JavaOperator, self).__init__(bash_command=command,
                                           *args, **kwargs)

    def execute(self, context):
        super(JavaOperator, self).execute(context)
