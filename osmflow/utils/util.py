import os
from airflow.models import AirflowException
from subprocess import Popen, STDOUT, PIPE
import logging


def get_variables(args):
    results = {}
    for k, v in args.items():
        if type(v) == str:
            results[k] = v
    return results


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def build_artifactory_url(name,
                          group,
                          version,
                          snapshot,
                          ext,
                          classifier=None
                          ):
    repo_name = 'libs-snapshot' if snapshot else 'libs-release'
    version = version + '-SNAPSHOT' if snapshot else version
    repo_name = 'libs-snapshot' if snapshot else 'libs-release'
    group = group.replace('.', '/')
    file_name = '{name}-{version}'
    if classifier:
        file_name += '-{classifier}'
    file_name += '.{ext}'
    file_name = file_name.format(**locals())
    url = 'https://artifacts.geo.apple.com/artifactory/{repo_name}/{group}/{name}/{version}/{file_name}'.format(
            **locals())
    return url


def run_bash(bash_command, cwd=None, env=None):
    sp = Popen(bash_command,
               shell=True,
               stdout=PIPE, stderr=STDOUT,
               cwd=cwd, env=env)

    logging.info("Output:")
    line = ''
    for line in iter(sp.stdout.readline, b''):
        line = line.strip()
        logging.info(line)
    sp.wait()
    logging.info("Command exited with "
                 "return code {0}".format(sp.returncode))

    if sp.returncode:
        raise AirflowException("Bash command failed")


def deploy(
        group=None,
        name=None,
        version=None,
        classifier=None,
        ext='jar',
        snapshot=False,
        dest_dir=None,
        unpack=False,
        cwd=None,
        env=None):
    """
    :param group:  artifact group name
    :param name:  artifact name
    :param version: artifact version
    :param classifier:  artifact classifier, in our cases like shaded, fat, etc..
    :param ext:  artifact extension, default is jar.
    :param snapshot: if using latest snapshot
    :param dest_dir: local dir to deploy to
    :param unpack:  if artifact is a zipped package, set unpack to True will unpack it
    :param cwd:  current working directory
    :param env:  environment variables
    :return:
    """
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

    run_bash(cmd, cwd, env)


def massilia_op(origin_url, dest_url, work_dir, app):
    main_class = 'com.apple.geo.osm.core.massilia.blob.MassiliaCommand'
    java_args = None
    src_remote = origin_url.startswith('massilia://')
    dest_remote = dest_url.startswith('massilia://') if dest_url else False
    if not dest_url and src_remote:
        op_args = ['-type=list', '-blobRead={}'.format(origin_url)]
    elif src_remote and not dest_remote:
        op_args = ['-type=download -blobRead={} -fileWrite={}'.format(origin_url, dest_url)]
    elif not src_remote and dest_remote:
        op_args = ['-type=upload -fileRead={} -blobWrite{}='.format(origin_url, dest_url)]
    elif src_remote and dest_remote:
        op_args = ['-type=copy -blobRead={} -blobWrite={}'.format(origin_url, dest_url)]
    else:
        raise AirflowException("invalid massilia operation, check src & dest")


class Runtime(object):
    def __init__(self, default_args):
        self.default_args = default_args

    def get(self, key):
        if key in os.environ:
            return expand_env_var(os.environ[key])
        elif key in self.default_args:
            return expand_env_var(self.default_args[key])
        else:
            return None



if __name__ == '__main__':
    deploy(group='com.apple.geo.osm')
