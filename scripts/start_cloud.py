from hydra.cluster_manager import ClusterManager


if __name__ == '__main__':
    cm =ClusterManager("local", 1, 4, 1, 2, 2)
    client = cm.start_cluster()
    loop = client.io_loop

