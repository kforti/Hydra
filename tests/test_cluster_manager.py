



def test_local_cluster():
    from hydra.cluster_manager import ClusterManager

    cm = ClusterManager(type="local")
    cm.start_cluster()
    cm.close()

if __name__ == '__main__':
    test_local_cluster()
