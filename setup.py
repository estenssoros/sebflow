from setuptools import find_packages, setup


def do_setup():
    setup(
        name='sebflow',
        description='programatically author, schedule, and monitor data pipelines',
        license='MIT',
        packages=find_packages(),
        install_requires=[
            'psycopg2==2.7.3.2',
            'django==1.11',
            'tabulate== 0.7.7'
        ],
        author='Sebastian Estenssoro',
        author_email='seb.estenssoro@gmail.com',
        url = 'http://estenssoros.com',
        scripts=['sebflow/bin/sebflow'],
    )


if __name__ == '__main__':
    do_setup()
