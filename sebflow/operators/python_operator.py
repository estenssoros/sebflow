from sebflow.exceptions import SebflowException
from sebflow.models import BaseOperator
from sebflow.state import State
# from sebflow.utils.decorators import apply_defaults


class PythonOperator(BaseOperator):
    # @apply_defaults
    def __init__(
        self,
        python_callable,
        op_args=None,
        op_kwargs=None,
            *args, **kwargs):
        super(PythonOperator, self).__init__(*args, **kwargs)
        if not callable(python_callable):
            raise SebflowException('`python_callable` must be callable')
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

    def execute(self):
        self.state = State.RUNNING
        self.logger.info('task %s [starting]' % self.task_id)
        return_value = self.execute_callable()
        return return_value

    def execute_callable(self):
        return self.python_callable(*self.op_args, **self.op_kwargs)
