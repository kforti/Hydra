from itertools import tee

from prefect import Flow
import cloudpickle

from hydra.tool import ExecuteCommand


class Pipeline:
    def __init__(self, name):
        self.name = name

        self._flow = Flow(name)

    def run(self, state=None, **kwargs):
        state = self._flow.run(**kwargs)
        with open("result.hydra", "wb") as f:
            cloudpickle.dump(state, f)
        return state


    def map_command(self, runner, cmd, input, output=None, upstream_tasks=None, upstream_cmds=None):
        try:
            inputs = iter(input)
            outputs = iter(output)
        except TypeError as te:
            raise Exception("Input or Output is not iterable")

        return self.add_parallel_command(runner, cmd, inputs, outputs, upstream_tasks=upstream_tasks, upstream_cmds=upstream_cmds)

    def add_command(self, runner, cmd, input, output=None, upstream_tasks=None, upstream_cmds=None):
        execute = ExecuteCommand(runner, cmd)
        # Workflow Definition
        with self._flow as flow:
            result = execute(input=input, output=output)
            if upstream_tasks:
                result.set_dependencies(upstream_tasks=upstream_tasks)
            if upstream_cmds:
                result.set_dependencies(upstream_tasks=upstream_cmds)

        return result

    def add_parallel_command(self, runner, cmd, inputs, outputs, upstream_tasks=None, upstream_cmds=None):
        execution_tasks = []
        inputs, inputs_copy = tee(inputs)
        for i in inputs_copy:
            execute = ExecuteCommand(runner, cmd)
            execution_tasks.append(execute)

        results = []
        # Workflow Definition
        with self._flow as flow:
            for i, (task, inp, out) in enumerate(zip(execution_tasks, inputs, outputs)):
                result = task(input=inp, output=out)
                if upstream_cmds:
                    result.set_dependencies(upstream_tasks=[cmds[i] for cmds in upstream_cmds])
                if upstream_tasks:
                    result.set_dependencies(upstream_tasks=[tasks[i] for tasks in upstream_tasks])
                results.append(result)
        return results

    def map_task(self, task, collection, upstream_tasks=None, upstream_cmds=None, **kwargs):
        results = []
        with self._flow as flow:
            for i, item in enumerate(collection):
                result = task(item, **kwargs)
                if upstream_cmds:
                    result.set_dependencies(upstream_tasks=[cmds[i] for cmds in upstream_cmds])
                if upstream_tasks:
                    result.set_dependencies(upstream_tasks=[tasks[i] for tasks in upstream_tasks])
                results.append(result)
        return results

    def add_task(self, task, **kwargs):
        # Workflow Definition
        with self._flow as flow:
            result = task(**kwargs)
        return result

    def visualize(self):
        self._flow.visualize()
