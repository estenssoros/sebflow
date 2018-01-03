from datetime import timedelta
from datetime.datetime import now

from sebflow.utils.dates import cron_presets


class DAG(object):
    def __init__(self,
                 dag_id,
                 description='',
                 schedule_interval=timedelta(days=1),
                 start_date=None, end_date=None,
                 full_filepath=None,
                 default_args=None  # TODO: build out default args
                 ):

        self.default_args = default_args or {}

        self._dag_id = dag_id
        self._full_filepath = full_filepath if full_filepath else ''
        self._description = description

        self.start_date = start_date
        self.end_date = end_date

        self.schedule_interval = schedule_interval
        if schedule_interval in cron_presets:
            self._schedule_interval = cron_presets.get(schedule_interval)
        elif schedule_interval == '@once':
            self._schedule_interval = None
        else:
            self._schedule_interval = schedule_interval

        self.last_loaded = now()

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)
