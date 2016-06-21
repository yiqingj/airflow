from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.hooks import BaseHook
from osmflow.utils.util import deploy, run_bash


class AtlasToPostgresTransfer(BaseOperator):
    template_fields = ('atlas_path',)
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            atlas_path='',
            postgres_conn_id='',
            application_package='',
            xcom_push=False,
            env=None,
            *args, **kwargs):
        self.postgres_conn_id = postgres_conn_id
        self.env = env
        self.bash_command = """
COUNTRY_SEPARATOR=","
NEW_LINE="\\n"
COUNTRY_ARRAY=(`echo $COUNTRIES | tr $COUNTRY_SEPARATOR $NEW_LINE`)
for COUNTRY in ${COUNTRY_ARRAY[@]}
do
    createdb atlas-{{ ds }} -p $DATABASE_PORT -h $DATABASE_HOST -U $DATABASE_USER -w
    java -cp :../package/libs/* com.apple.geo.osm.core.atlas.db.cli.AtlasDbCLI \
    -atlas=$COUNTRY \
    -db_url=$DATABASE_HOST:$DATABASE_PORT:atlas-{{ ds }}:$COUNTRY:$DATABASE_USER:$DATABASE_PASSWORD
done
        """

    def execute(self, context):
        work_dir = context['base_dir']
        deploy(work_dir=work_dir,
               name='osm-pipeline',
               group='com.apple.geo.osm',
               version='1.3.15',
               ext='zip',
               dest_dir='app',
               unpack=True
               )
        conn = BaseHook.get_connection(self.postgres_conn_id)

        run_bash(self.bash_command, work_dir, self.env)
