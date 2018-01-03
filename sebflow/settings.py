import os
import sys
import pendulum

DB_CREDS = {
    'user': 'seb',
    'passwd': 'sebflow132435',
    'host': '127.0.0.1',
    'db': 'sebflow'

}


SEBFLOW_HOME = os.environ.get('SEBFLOW_HOME')

if SEBFLOW_HOME is None:
    print('could not find environment variable SEBFLOW_HOME')
    sys.exit(1)

DAG_FOLDER = os.path.join(SEBFLOW_HOME,'dags')

if not os.path.exists(DAG_FOLDER):
    print('Could not locate dag folder in %s' % settings.SEBFLOW_HOME)
    sys.exit(1)
TIMEZONE = TIMEZONE = pendulum.timezone('UTC')

HEADER = '''
   _____ __________  ________    ____ _       __
  / ___// ____/ __ )/ ____/ /   / __ \ |     / /
  \__ \/ __/ / __  / /_  / /   / / / / | /| / /
 ___/ / /___/ /_/ / __/ / /___/ /_/ /| |/ |/ /
/____/_____/_____/_/   /_____/\____/ |__/|__/

'''
print(HEADER)
