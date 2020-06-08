from hydra.tool import Tool


tool = Tool.load("call", "/home/kevin/PycharmProjects/hydra-tools/tool_set.yaml")
print(tool)
