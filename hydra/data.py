


class Data:
    def __init__(self, input, output):
        pass


class ParallelData:
    def __init__(self, inputs, outputs):
        self.inputs = inputs
        self.outputs = outputs

    def replace_inputs(self, old, new):
        inputs = [i.replace(old, new) for i in self.inputs]
        return inputs

    def replace_outputs(self, old, new):
        outputs = [i.replace(old, new) for i in self.outputs]
        return outputs

    def map_inputs(self, template):
        inputs = []
        for i, data in enumerate(self.inputs):
            inp = template.format(partition=i)
            inputs.append(inp)
        return inputs

    def map_outputs(self, template):
        outputs = []
        for i, data in enumerate(self.outputs):
            inp = template.format(partition=i)
            outputs.append(inp)
        return outputs

if __name__ == '__main__':
    albacore_input = [
        "/home/kevin/bin/hydra_nanopore/tests/test_data/minion_sample_raw_data/Experiment_01/sample_02_local/pass/2",
        "/home/kevin/bin/hydra_nanopore/tests/test_data/minion_sample_raw_data/Experiment_01/sample_02_local/pass/3",
        "/home/kevin/bin/hydra_nanopore/tests/test_data/minion_sample_raw_data/Experiment_01/sample_02_local/pass/4",
        "/home/kevin/bin/hydra_nanopore/tests/test_data/minion_sample_raw_data/Experiment_01/sample_02_local/pass/5"]
    albacore_output = ["/home/kevin/bin/hydra_nanopore/tests/test_data/output/2",
                       "/home/kevin/bin/hydra_nanopore/tests/test_data/output/3",
                       "/home/kevin/bin/hydra_nanopore/tests/test_data/output/4",
                       "/home/kevin/bin/hydra_nanopore/tests/test_data/output/5"]
    minimap2_output = ["/home/kevin/bin/hydra_nanopore/tests/test_data/lambda_2.sam",
                       "/home/kevin/bin/hydra_nanopore/tests/test_data/lambda_3.sam",
                       "/home/kevin/bin/hydra_nanopore/tests/test_data/lambda_4.sam",
                       "/home/kevin/bin/hydra_nanopore/tests/test_data/lambda_5.sam"]
    parallel = ParallelData(albacore_input, albacore_output)
    d = ParallelData(inputs=parallel.outputs,
                     outputs=parallel.map_outputs("/home/kevin/bin/hydra_nanopore/tests/test_data/lambda_{partition}.sam"))
    print(d.outputs)



