import os
import sys

import MySQLdb
from sebflow import settings



def init_db():

    dags = [f for f in os.listdir(settings.DAG_FOLDER) if f.endswith('.py')]
    if not dags:
        print('No dags present in %s' % dag_folder)
        sys.exit(1)

    conn = MySQLdb.connect(**settings.DB_CREDS)
    curs = conn.cursor()

    curs.execute('DROP TABLE IF EXISTS dags;')
    conn.commit()

    curs.execute('''
    CREATE TABLE dags (
        id SERIAL,
        description TEXT,
        schedule_interval VARCHAR(10),
        start_date DATE DEFAULT CURRENT_TIMESTAMP,
        end_date DATE,
        last_run DATETIME DEFAULT NULL,
        success TINYINT(1)
    )
    ''')

    conn.close()
    curs.close()
