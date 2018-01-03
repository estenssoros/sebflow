from sebflow import settings
import MySQLdb


def init_db():
    conn = MySQLdb.connect(**settings.DB_CREDS)
    curs = conn.cursor()

    curs.execute('DROP TABLE IF EXISTS dags;')
    conn.commit()

    curs.execute('''
    CREATE TABLE dags (
        id SERIAL,
        name VARCHAR(50),
        last_run DATETIME DEFAULT NULL,
        success TINYINT(1)
    )
    ''')

    conn.close()
    curs.close()
