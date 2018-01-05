import os
import shutil

from setuptools import find_packages, setup

for f in ('build', 'dist', 'sebflow.egg-info'):
    if os.path.exists(f):
        shutil.rmtree(f)


def do_setup():
    setup(
        name='sebflow',
        description='programatically author, schedule, and monitor data pipelines',
        license='MIT',
        packages=find_packages(),
        include_package_date =True,
        install_requires=[
            'psycopg2==2.7.3.2',
            'django==1.11',
            'tabulate==0.7.7',
            'sqlalchemy-utc==0.9.0'
        ],
        author='Sebastian Estenssoro',
        author_email='seb.estenssoro@gmail.com',
        url='http://estenssoros.com',
        scripts=['sebflow/bin/sebflow'],
        entry_points={'console_scripts':
                      ['sebflow = sebflow.bin.sebflow:entrypoint']
                      },
    )


if __name__ == '__main__':
    do_setup()
