from setuptools import setup, find_packages

setup(
        name='airflow',
        description='OSM pipeline server backend',
        version='0.1.1',
        packages=find_packages(),
        package_data={'': ['airflow/alembic.ini']},
        include_package_data=True,
        zip_safe=False,
        scripts=['airflow/bin/airflow'],
        install_requires=[
            'flask',
            'flask-restful',
            'flask-script',
            'Flask-Migrate',
            'alembic',
            'sqlalchemy',
            'future',
            'psycopg2'

        ],
        author='Yiqing Jin',
        author_email='yiqing_jin@apple.com',
        url='https://github.geo.apple.com/yiqing-jin/openflow-server')
