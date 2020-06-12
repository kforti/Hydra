import uuid
from itertools import tee

from hydra.data import ParallelData, Data
from hydra.runners.base import Runner
import prefect
import cloudpickle
from hydra.runners.shell import ShellRunner

from hydra.tool import ExecuteCommand, Command
from typing import List, Union, Dict


class TransformTicket:
    def __init__(self, id, transform, map=False, data=None, tasks=None):
        self.id = id
        self.data = data
        self.tasks = tasks
        self.transform = transform
        self.map = map


class PipelineBuilder:
    def __init__(self, name=None, pipeline=None, default_runner=ShellRunner):
        if not pipeline and not name:
            raise Exception("Must provide a name for the Pipeline")
        if not pipeline:
            pipeline = Pipeline(name)

        self._pipeline = pipeline
        self._transform_flow = prefect.Flow(self._pipeline.name + "_transforms")
        self._lineage = {}
        self.default_runner = default_runner

    def add_command(self,
                    data: Union[Data, ParallelData]=None,
                    cmd: Command=None,
                    runner: Runner=None,
                    upstream_transforms: List=None,
                    downstream_transforms: List=None,
                    **kwargs):
        upstream_tasks = None
        downstream_tasks = None
        if upstream_transforms:
            upstream_tasks = [self._lineage[i.id].tasks for i in upstream_transforms]
        if downstream_transforms:
            downstream_tasks = [self._lineage[i.id].tasks for i in downstream_transforms]

        if not kwargs:
            kwargs = {}

        if data:
            kwargs.update(data.mapping)

        if not runner:
            runner = self.default_runner

        task = ExecuteCommand(runner=runner, command=cmd)

        task = self._pipeline.add_task(
            task, upstream_tasks=upstream_tasks, downstream_tasks=downstream_tasks, **kwargs)

        ticket = self._add_transform(task, upstream_transforms, downstream_transforms, **kwargs)
        ticket.map = False
        ticket.data = data
        ticket.tasks = task

        self._lineage[ticket.id] = ticket

        return ticket

    def map_command(self,
                    data: Union[Data, ParallelData]=None,
                    cmd: Command=None,
                    runner: Runner=None,
                    iterables: Dict=None,
                    upstream_transforms: List=None,
                    downstream_transforms: List=None,
                    **kwargs):
        upstream_tasks = None
        downstream_tasks = None
        if upstream_transforms:
            upstream_tasks = [self._lineage[i.id].tasks for i in upstream_transforms]
        if downstream_transforms:
            downstream_tasks = [self._lineage[i.id].tasks for i in downstream_transforms]

        if not kwargs:
            kwargs = {}

        if not iterables:
            iterables = {}

        if data:
            iterables.update(data.mapping)

        if not runner:
            runner = self.default_runner

        task = ExecuteCommand(runner=runner, command=cmd)

        tasks = self._pipeline.map_task(
            task, iterables=iterables, upstream_tasks=upstream_tasks, downstream_tasks=downstream_tasks, **kwargs)

        keys = iterables.keys()
        kwargs.update(dict.fromkeys(keys))
        ticket = self._add_transform(task, upstream_transforms, downstream_transforms, **kwargs)
        ticket.map = True
        ticket.data = data
        ticket.tasks = tasks

        self._lineage[ticket.id] = ticket
        return ticket

    def _add_transform(self, task, upstream_transforms=None, downstream_transforms=None, **kwargs):
        upstream_tasks = None
        downstream_tasks = None
        if downstream_transforms:
            downstream_tasks = [i.transform for i in downstream_transforms]
        if upstream_transforms:
            upstream_tasks = [i.transform for i in upstream_transforms]
        with self._transform_flow as flow:
            result = task(**kwargs)
            if upstream_tasks or downstream_tasks:
                result.set_dependencies(upstream_tasks=upstream_tasks, downstream_tasks=downstream_tasks)
        ticket = TransformTicket(id="{name}-{id}".format(name=task.name, id=uuid.uuid4()),
                                 transform=result)

        return ticket

    def add_task(self,
                 task,
                 data: Union[Data, ParallelData]=None,
                 upstream_transforms: List=None,
                 downstream_transforms: List=None,
                 **kwargs):
        upstream_tasks = None
        downstream_tasks = None
        if upstream_transforms:
            upstream_tasks = [self._lineage[i.id].tasks for i in upstream_transforms]
        if downstream_transforms:
            downstream_tasks = [self._lineage[i.id].tasks for i in downstream_transforms]

        if not kwargs:
            kwargs = {}

        if data:
            kwargs.update(data.mapping)

        task = self._pipeline.add_task(
            task, upstream_tasks=upstream_tasks, downstream_tasks=downstream_tasks, **kwargs)

        ticket = self._add_transform(task, upstream_transforms, downstream_transforms, **kwargs)
        ticket.map = False
        ticket.data = data
        ticket.tasks = task

        self._lineage[ticket.id] = ticket

        return ticket

    def map_task(self,
                 task,
                 data: Union[Data, ParallelData]=None,
                 iterables: Dict=None,
                 upstream_transforms=None,
                 downstream_transforms=None,
                 **kwargs):
        upstream_tasks = None
        downstream_tasks = None
        if upstream_transforms:
            upstream_tasks = [self._lineage[i.id].tasks for i in upstream_transforms]
        if downstream_transforms:
            downstream_tasks = [self._lineage[i.id].tasks for i in downstream_transforms]

        if not kwargs:
            kwargs = {}

        if not iterables:
            iterables = {}

        if data:
            iterables.update(data.mapping)

        tasks = self._pipeline.map_task(task, iterables, upstream_tasks, downstream_tasks, **kwargs)

        kwargs.update(iterables)
        ticket = self._add_transform(task, upstream_transforms, downstream_transforms, **kwargs)
        ticket.map = True
        ticket.data = data
        ticket.tasks = tasks

        self._lineage[ticket.id] = ticket
        return ticket

    def visualize_transforms(self):
        self._transform_flow.visualize()

    def visualize_tasks(self):
        self._pipeline.visualize()


class Pipeline:
    def __init__(self, name):
        self.name = name
        self._flow = prefect.Flow(name)

    def run(self, state=None, **kwargs):
        state = self._flow.run(**kwargs)
        with open("result.hydra", "wb") as f:
            cloudpickle.dump(state, f)
        return state

    def add_task(self,
                 task: prefect.Task = None,
                 upstream_tasks: List=None,
                 downstream_tasks: List=None,
                 **kwargs) -> TransformTicket:
        """ """
        with self._flow as f:
            result = task(**kwargs)
            if upstream_tasks or downstream_tasks:
                result.set_dependencies(upstream_tasks=upstream_tasks, downstream_tasks=downstream_tasks)
        return result


    def map_task(self, task, iterables: Dict=None, upstream_tasks=None, downstream_tasks=None, **kwargs):

        keys = list(iterables.keys())
        if len(keys) > 1:
            collections = list(iterables.values())
            collections = zip(*collections)
        elif len(keys) == 1:
            collections = list(*iterables.values())
        results = []
        with self._flow as flow:
            for i, data in enumerate(collections):
                params = dict.fromkeys(keys)
                for k, key in enumerate(keys):
                    params[key] = data[k]
                if kwargs:
                    params.update(kwargs)

                result = task(**params)
                if upstream_tasks:
                    result.set_dependencies(upstream_tasks=[tasks[i] for tasks in upstream_tasks])
                if downstream_tasks:
                    result.set_dependencies(downstream_tasks=[tasks[i] for tasks in downstream_tasks])
                results.append(result)
        return results


    def visualize(self):
        self._flow.visualize()

