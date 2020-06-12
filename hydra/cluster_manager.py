import asyncio
import math

from distributed import SchedulerPlugin, WorkerPlugin, Client, Worker

from hydra.executor import HydraExecutor


class ClusterManager:

    def __init__(self, type=None,
                 tasks_per_worker=None,
                 num_workers=None,
                 workers_per_job=None,
                 memory_per_worker=None,
                 cores_per_worker=None,
                 clusters=None,
                 num_jobs=None,
                 min_num_workers=None,
                 image="daskdev/dask:latest",
                 **kwargs):
        self.cluster = None
        self.executor = None
        self.client = None

        self.type = type
        self.cluster_kwargs = kwargs
        self.tasks_per_worker = tasks_per_worker
        self.num_workers = num_workers
        self.num_jobs = num_jobs
        self.workers_per_job = workers_per_job
        self.memory_per_worker = memory_per_worker
        self.cores_per_worker = cores_per_worker
        self.min_num_workers = min_num_workers
        self.image = image

        self.clusters = {
            "local": self._build_local,
            "lsf": self._build_lsf,
            "slurm": self._build_slurm,
            "kubernetes": self._build_kubernetes
        }
        if clusters:
            self.clusters.update(clusters)

    def start_cluster(self):
        if self.type not in self.clusters:
            raise KeyError("this cluster type is not supported")
        build_cluster = self.clusters[self.type]
        self.cluster = build_cluster()

        self.client = Client(self.cluster.scheduler_address)

        minimum_workers = self.min_num_workers or int(0.5 * self.num_workers)
        self.cluster.scale(self.num_workers)
        self.client.wait_for_workers(minimum_workers)
        self.cluster.adapt(minimum=minimum_workers, maximum=self.num_workers)

        return self.client

    def close_cluster(self):
        if self.cluster:
            self.cluster.close()

    def create_executor(self, executor):
        executor = executor(address=self.cluster.scheduler_address,
                            security=self.cluster.security)
        return executor

    def _build_local(self):
        from distributed import LocalCluster

        cluster = LocalCluster(n_workers=self.num_workers,
                               threads_per_worker=self.tasks_per_worker,
                               **self.cluster_kwargs)
        return cluster

    def _build_lsf(self):
        from dask_jobqueue import LSFCluster
        num_jobs = math.ceil(self.num_workers / self.workers_per_job)
        cores = self.workers_per_job * self.cores_per_worker
        memory = self.workers_per_job * self.memory_per_worker
        jextra = ['-R rusage[mem={}]'.format(self.memory or "")]

        if "job_extra" in self.cluster_kwargs:
            self.cluster_kwargs["job_extra"].extend(jextra)
        elif "job_extra" not in self.cluster_kwargs:
            self.cluster_kwargs["job_extra"] = jextra

        cluster = LSFCluster(n_workers=self.num_workers,
                             processes=self.workers_per_job,
                             cores=cores,
                             memory=memory,
                             ncpus=cores,
                             **self.cluster_kwargs)

        return cluster

    def _build_slurm(self):
        pass

    def _build_kubernetes(self):
        from dask_kubernetes import KubeCluster, make_pod_spec
        memory = self.memory_per_worker * self.workers_per_job
        ncpus = self.cores_per_worker * self.workers_per_job
        spec = make_pod_spec(image=self.image,
                             threads_per_worker=self.tasks_per_worker,
                             memory_limit=memory,
                             memory_request=memory,
                             cpu_limit=ncpus,
                             cpu_request=ncpus)
        cluster = KubeCluster(spec, n_workers=self.num_workers, **self.cluster_kwargs)

        return cluster



#
#
#
# if __name__ == '__main__':
#     from distributed import LocalCluster, Client, Scheduler
#     import threading
#
#
#     cluster = LocalCluster()
#     client = Client(cluster.scheduler_address)
#
#     plugin = ReportWorkerPlugin()
#     client.register_worker_plugin(plugin)

