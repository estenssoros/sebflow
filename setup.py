import os
import shutil

from setuptools import find_packages, setup

base_dir = os.path.dirname(__file__)


def remove_build_files():
    for f in ('build', 'dist', 'sqlwriter.egg-info'):
        if os.path.exists(f):
            shutil.rmtree(f)


with open(os.path.join(base_dir, "README.rst")) as f:
    long_description = f.read()


def do_setup():
    remove_build_files()
    setup(
        name='sebflow',
        version='0.0.2',
        license='MIT',
        description='programatically author and monitor data pipelines',
        long_description=long_description,
        packages=find_packages(),
        include_package_data=True,
        install_requires=[
            'psycopg2==2.7.3.2',
            'django==2.2.24',
            'tabulate==0.7.7',
            'sqlalchemy-utc==0.9.0',
            'termcolor==1.1.0',
            'colorama==0.3.9',
            'flask==0.11',
            'flask-wtf==0.14',
            'flask-cache==0.13.1',
            'markdown==2.5.2',
            'flask-admin==1.4.1',
            'jinja2>=2.7.3, <2.9.0',
            'bleach==2.1.2',
            'python-nvd3==0.14.2',
            'python-slugify==1.1.4'
        ],
        author='Sebastian Estenssoro',
        author_email='seb.estenssoro@gmail.com',
        url='https://github.com/estenssoros/sebflow',
        scripts=['sebflow/bin/sebflow'],
        entry_points={'console_scripts':
                      ['sebflow = sebflow.bin.sebflow:entrypoint']
                      },
    )


if __name__ == '__main__':
    do_setup()
