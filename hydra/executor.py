import datetime
import logging
import queue
import uuid
import warnings
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator, List

import dask
from distributed import Client, Future, fire_and_forget, worker_client

from prefect import context
from prefect.engine.executors.base import Executor

from cadence.workflow import workflow_method, Workflow, WorkflowClient

def get_workflow(task_list):
    # Workflow Interface
    class GreetingWorkflow:
        @workflow_method(execution_start_to_close_timeout_seconds=10, task_list=task_list)
        async def get_greeting(self, name: str) -> str:
            raise NotImplementedError
    return GreetingWorkflow


class HydraExecutor(Executor):
    """
    An executor that runs all functions using the `dask.distributed` scheduler on
    a (possibly local) dask cluster.  If you already have one running, provide the
    address of the scheduler upon initialization; otherwise, one will be created
    (and subsequently torn down) within the `start()` contextmanager.

    Note that if you have tasks with tags of the form `"dask-resource:KEY=NUM"` they will be parsed
    and passed as [Worker Resources](https://distributed.dask.org/en/latest/resources.html) of the form
    `{"KEY": float(NUM)}` to the Dask Scheduler.

    Args:
        - address (string, optional): address of a currently running dask
            scheduler; if one is not provided, a `distributed.LocalCluster()` will be created in `executor.start()`.
            Defaults to `None`
        - local_processes (bool, optional): whether to use multiprocessing or not
            (computations will still be multithreaded). Ignored if address is provided.
            Defaults to `False`.
        - debug (bool, optional): whether to operate in debug mode; `debug=True`
            will produce many additional dask logs. Defaults to the `debug` value in your Prefect configuration
        - **kwargs (dict, optional): additional kwargs to be passed to the
            `dask.distributed.Client` upon initialization (e.g., `n_workers`)
    """

    def __init__(
        self,
        address: str = None,
        local_processes: bool = None,
        debug: bool = None,
        control_address: str = None,
        **kwargs: Any
    ):
        if address is None:
            address = context.config.engine.executor.dask.address
        if address == "local":
            address = None
        if local_processes is None:
            local_processes = context.config.engine.executor.dask.local_processes
        if debug is None:
            debug = context.config.debug
        self.control_address = control_address
        self.address = address
        self.local_processes = local_processes
        self.debug = debug
        self.is_started = False
        self.kwargs = kwargs
        super().__init__()

    @contextmanager
    def start(self) -> Iterator[None]:
        """
        Context manager for initializing execution.

        Creates a `dask.distributed.Client` and yields it.
        """
        try:
            if self.address is None:
                self.kwargs.update(
                    silence_logs=logging.CRITICAL if not self.debug else logging.WARNING
                )
                self.kwargs.update(processes=self.local_processes)
            self.client = WorkflowClient.new_client(domain=DOMAIN)

            # result = greeting_workflow.get_greeting("Python")
        #     with Client(self.address, **self.kwargs) as client:
        #         self.hydra_client = Client(self.control_address)
        #         self.client = client
        #         self.is_started = True
            yield self.client
        finally:
            self.client = None
            self.is_started = False

    def _prep_dask_kwargs(self) -> dict:
        dask_kwargs = {"pure": False}  # type: dict

        ## set a key for the dask scheduler UI
        if context.get("task_full_name"):
            key = context.get("task_full_name", "") + "-" + str(uuid.uuid4())
            dask_kwargs.update(key=key)

        ## infer from context if dask resources are being utilized
        dask_resource_tags = [
            tag
            for tag in context.get("task_tags", [])
            if tag.lower().startswith("dask-resource")
        ]
        if dask_resource_tags:
            resources = {}
            for tag in dask_resource_tags:
                prefix, val = tag.split("=")
                resources.update({prefix.split(":")[1]: float(val)})
            dask_kwargs.update(resources=resources)

        return dask_kwargs

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        if "client" in state:
            del state["client"]
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)

    def submit(self, fn: Callable, *args: Any, **kwargs: Any) -> Future:
        """
        Submit a function to the executor for execution. Returns a Future object.

        Args:
            - fn (Callable): function that is being submitted for execution
            - *args (Any): arguments to be passed to `fn`
            - **kwargs (Any): keyword arguments to be passed to `fn`

        Returns:
            - Future: a Future-like object that represents the computation of `fn(*args, **kwargs)`
        """
        task = kwargs["task"]
        workflow = self.client.new_workflow_stub(get_workflow(task.worker_id))
        dask_kwargs = self._prep_dask_kwargs()
        kwargs.update(dask_kwargs)

        if self.is_started and hasattr(self, "client"):
            if hasattr(task, "hydra_task"):
                self.hydra_client.submit(fn, *args, **kwargs)
            else:
                future = self.client.submit(fn, *args, **kwargs)
        elif self.is_started:
            with worker_client(separate_thread=True) as client:
                future = client.submit(fn, *args, **kwargs)
        else:
            raise ValueError("This executor has not been started.")

        fire_and_forget(future)
        return future

    def map(self, fn: Callable, *args: Any, **kwargs: Any) -> List[Future]:
        """
        Submit a function to be mapped over its iterable arguments.

        Args:
            - fn (Callable): function that is being submitted for execution
            - *args (Any): arguments that the function will be mapped over
            - **kwargs (Any): additional keyword arguments that will be passed to the Dask Client

        Returns:
            - List[Future]: a list of Future-like objects that represent each computation of
                fn(*a), where a = zip(*args)[i]

        """
        if not args:
            return []

        dask_kwargs = self._prep_dask_kwargs()
        kwargs.update(dask_kwargs)

        if self.is_started and hasattr(self, "client"):
            futures = self.client.map(fn, *args, **kwargs)
        elif self.is_started:
            with worker_client(separate_thread=True) as client:
                futures = client.map(fn, *args, **kwargs)
                return client.gather(futures)
        else:
            raise ValueError("This executor has not been started.")

        fire_and_forget(futures)
        return futures

    def wait(self, futures: Any) -> Any:
        """
        Resolves the Future objects to their values. Blocks until the computation is complete.

        Args:
            - futures (Any): single or iterable of future-like objects to compute

        Returns:
            - Any: an iterable of resolved futures with similar shape to the input
        """
        if self.is_started and hasattr(self, "client"):
            return self.client.gather(futures)
        elif self.is_started:
            with worker_client(separate_thread=True) as client:
                return client.gather(futures)
        else:
            raise ValueError("This executor has not been started.")
