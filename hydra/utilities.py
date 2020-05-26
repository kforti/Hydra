import os

import cloudpickle


def save_flow_state(state, name, path):
    save_path = os.path.join(path, name + ".hydra")
    with open(path, "wb") as f:
        cloudpickle.dump(state, f)
