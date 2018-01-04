from sebflow import settings


class BaseExecutor(object):
    def __init__(self, parallelism=settings.PARALLELISM):
        self.parallelism = parallelism
        self.queued_tasks = {}
        self.running = {}
        self.event_buffer = {}

    def start(self):
        pass

    def queue_command(self, task_instance, command, priority=1, queue=None):
        key = task_instance.key
        if key not in self.queued_tasks and key not in self.running:
            print('adding to queue: %s' % command)
            self.queued_tasks[key] = (command, priority, queue, task_instance)

    def queue_task_instance(self, task_instance, mark_success, pool=None):
        pool = pool or task_instance.pool
        command = task_instance.command(
            local=True,
            mark_success=mark_success,
            pool=pool)
        self.queue_command(task_instance, command, priority=task_instance.task.priority_weight_total, queue=task_instance.task.queue)

    def has_task(self, task_instance):
        if task_instance.key in self.queued_tasks or task_instance.key in self.running:
            return True

    def sync(self):
        pass

    def heartbeat(self):
        if not self.parallelism:
            open_slots = len(self.queued_tasks)
        else:
            open_slots = self.parallelism - len(self.running)
        print('%s running task instances' % len(self.running))
        print('%s in queue', len(self.queued_tasks))
        print('%s open slots', open_slots)

        sorted_queue = sorted([(k, v) for k, v in self.queued_tasks.items()], key=lambda x: x[1][1], reverse=True)
        for i in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, ti) = sorted_queue.pop(0)
            self.queued_tasks.pop(key)
            ti.refresh_from_db()
            if ti.state != state.RUNNING:
                self.running[key] = command
                self.execute_async(key, command=command, queue=queue)
            else:
                print('task already running, no sending to executor' % key)

        print('calling the sync method')
        self.sync()

    def change_state(self, key, state):
        self.running.pop(key)
        self.event_buffer[key] = state

    def fail(self, key):
        self.change_state(key, State.FAILED)

    def success(self, key):
        self.change_state(key, State.SUCCESS)

    def get_event_buffer(self, dag_ids=None):
        cleared_events = dict()
        if dag_ids is None:
            cleared_events = self.event_buffer
            self.event_buffer = dict()
        else:
            for key in list(self.event_buffer.keys()):
                dag_id, _, _ = key
                if dag_id in dag_ids:
                    cleared_events[key] = self.event_buffer.pop(key)
        return cleared_events

    def execute_async(self, key, command, queue=None):
        raise NotImplementedError()

    def end(self):
        raise NotImplementedError()

    def terminate(self):
        raise NotImplementedError()
