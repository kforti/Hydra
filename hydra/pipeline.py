from itertools import tee

from prefect import Flow

from hydra.tool import ExecuteCommand


class Pipeline:
    def __init__(self, name):
        self.name = name

        self._flow = Flow(name)

    def run(self, **kwargs):
        state = self._flow.run(**kwargs)
        return state


    def add_command(self, runner, cmd, input, output, upstream_depens=None, dependencies=None):
        try:
            inputs = iter(input)
            outputs = iter(output)
            parallel = True
        except TypeError as te:
            parallel = False

        if parallel:
            return self.add_parallel_command(runner, cmd, inputs, outputs, upstream_depens=upstream_depens)

        execute = ExecuteCommand(runner, cmd, input, output)
        # Workflow Definition
        with self._flow as flow:
            result = execute()

        return result

    def add_parallel_command(self, runner, cmd, inputs, outputs, upstream_depens=None, dependencies=None):
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
                if upstream_depens:
                    result.set_dependencies(upstream_tasks=[upstream_depens[i]])
                results.append(result)
        return results

    def map_task(self, task, collection, upstream_depens=None, **kwargs):
        results = []
        with self._flow as flow:
            for i, item in enumerate(collection):
                result = task(item, **kwargs)
                result.set_dependencies(upstream_tasks=[upstream_depens[i]])
                results.append(result)
        return results

    def add_task(self, task, **kwargs):
        # Workflow Definition
        with self._flow as flow:
            result = task(**kwargs)
        return result

    def visualize(self):
        self._flow.visualize()
