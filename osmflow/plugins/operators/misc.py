import os
from airflow.operators.bash_operator import BashOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException

from osmflow.utils.util import build_artifactory_url


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


class MassiliaOperator(JavaOperator):
    @apply_defaults
    def __init__(self,
                 src=None,
                 dest=None,
                 # mode='list',  # list, download, upload
                 *args,
                 **kwargs):

        main_class = 'com.apple.geo.osm.core.massilia.blob.MassiliaCommand'
        java_args = None
        src_remote = src.startswith('massilia://')
        dest_remote = dest.startswith('massilia://') if dest else False
        if not dest and src_remote:
            op_args = ['-type=list', '-blobRead={}'.format(src)]
        elif src_remote and not dest_remote:
            op_args = ['-type=download -blobRead={} -fileWrite={}'.format(src, dest)]
        elif not src_remote and dest_remote:
            op_args = ['-type=upload -fileRead={} -blobWrite{}='.format(src, dest)]
        elif src_remote and dest_remote:
            op_args = ['-type=copy -blobRead={} -blobWrite={}'.format(src, dest)]
        else:
            raise AirflowException("invalid massilia operation, check src & dest")
        super(MassiliaOperator, self).__init__(
                main_class=main_class,
                op_args=op_args,
                *args, **kwargs)

    def execute(self, context):
        super(MassiliaOperator, self).execute(context)


class ArtifactoryOperator(BashOperator):
    @apply_defaults
    def __init__(self,
                 name=None,
                 version=None,
                 classifier=None,
                 ext=None,
                 snapshot=False,
                 dest_dir=None,
                 group='',
                 user=None,
                 password=None,
                 unpack=False,
                 *args,
                 **kwargs):
        url = build_artifactory_url(
                name=name,
                group=group,
                version=version,
                classifier=classifier,
                ext=ext,
                snapshot=snapshot
        )
        version = version + '-SNAPSHOT' if snapshot else version

        group = group.replace('.', '/')
        file_name = '{name}-{version}'
        if classifier:
            file_name += '-{classifier}'
        file_name += '.{ext}'
        file_name = file_name.format(**locals())
        cmd = 'wget --no-verbose -P {dest_dir} --user $ARTIFACTORY_USER --password $ARTIFACTORY_PASSWORD {url}'.format(
                **locals())
        if unpack:
            if ext == 'zip':
                cmd += '\n unzip {dest_dir}/{file_name}'
        super(ArtifactoryOperator, self).__init__(
                bash_command=cmd,
                *args,
                **kwargs)
