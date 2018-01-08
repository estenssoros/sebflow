class State(object):
    PENDING = 'pending'
    QUEUED = 'queued'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILED = 'failed'
    UPSTREAM_FAILED = "upstream_failed"

    task_states = (
        SUCCESS,
        RUNNING,
        FAILED,
        UPSTREAM_FAILED,
        QUEUED,
        PENDING
    )

    dag_states = (
        SUCCESS,
        RUNNING,
        FAILED
    )

    state_color = {
        QUEUED: 'cyan',
        RUNNING: 'blue',
        SUCCESS: 'green',
        FAILED: 'red',
        PENDING: 'magenta',
        UPSTREAM_FAILED: 'yellow',

    }
    @classmethod
    def color(cls,state):
        if state in cls.state_color:
            return cls.state_color[state]
        else:
            return 'white'


    @classmethod
    def finished(cls):
        return [
            cls.SUCCESS,
            cls.FAILED
        ]

    @classmethod
    def unfinished(cls):
        return [
        cls.QUEUED,
        cls.RUNNGING,
        cls.PENDING
        ]
