class State(object):
    # scheduler
    NONE = None
    REMOVED = 'removed'
    SCHEDULED = 'scheduled'

    # set by task
    QUEUED = 'queued'
    RUNNING = 'running'
    SUCCESS = 'success'
    SHUTDOWN = 'shutdown'
    FAILED = 'failed'
    UP_FOR_RETRY = 'up_for_retry'
    UPSTREAM_FAILED = 'upsream_failed'
    SKIPPED = 'skipped'

    task_states = (
        SUCCESS,
        RUNNING,
        FAILED,
        UPSTREAM_FAILED,
        UP_FOR_RETRY,
        QUEUED
    )

    dag_states = (
        SUCCESS,
        RUNNING,
        FAILED
    )

    state_color = {
        QUEUED: 'gray',
        RUNNING: 'lime',
        SUCCESS: 'green',
        SHUTDOWN: 'blue',
        FAILED: 'red',
        UP_FOR_RETRY: 'gold',
        UPSTREAM_FAILED: 'orange',
        SKIPPED: 'pink',
        REMOVED: 'lightgrey',
        SCHEDULED: 'white'
    }

    @classmethod
    def color(cls, state):
        if state in cls.state_color:
            return cls.state_color[state]
        else:
            return 'white'

    @classmethod
    def colof_fg(cls, state):
        color = cls.color(state)
        if color in ('green', 'red'):
            return 'white'
        else:
            return 'black'

    @classmethod
    def finished(cls):
        return [
            cls.SUCCESS,
            cls.SHUTDOWN,
            cls.FAILED,
            cls.SKIPPED
        ]

    @classmethod
    def unfinished(cls):
        return [
            cls.NONE,
            cls.SCHEDULED,
            cls.QUEUED,
            cls.RUNNING,
            cls.UP_FOR_RETRY
        ]
