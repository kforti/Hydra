import prefect
import yaml
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from jinja2 import Template


class Tool:
    def __init__(self, name, commands=None):
        self.name = name
        self.commands = commands

    def get_command(self, name, params=None):
        template = self.commands[name]["template"].strip()
        return Command(name, template, params)

    @classmethod
    def commands_from_yaml(cls, name, path):
        with open(path, "r") as f:
            tool_set = yaml.safe_load(f)
        commands = tool_set[name]
        return commands

    @classmethod
    def load_command(cls, name, command, path=None, params=None):
        if not path:
            path = "tool_set.yaml"
        commands = cls.commands_from_yaml(name, path)
        template = commands[command]["template"].strip()
        return Command(command, template, params)

    @classmethod
    def load(cls, name, path=None):
        if not path:
            path = "tool_set.yaml"
        commands = cls.commands_from_yaml(name, path)
        tool = cls(name, commands)
        return tool


class Command:
    def __init__(self, name, template, params=None):
        self.name = name
        self.template = template
        self.params = params or {}

        self._template = Template(self.template)

    def __call__(self, input, output, **kwargs):
        self.params.update(kwargs)
        command = self._template.render(input=input, output=output, **self.params)
        clean_command = command.replace('\n', '').replace('    ', ' ').replace('   ', ' ').replace('  ', ' ')
        print(clean_command)
        return clean_command


class ExecuteCommand(Task):
    def __init__(self, runner, command=None, input=None, output=None, success_fn=None, **kwargs):
        self.runner = runner
        self.command = command
        self.input = input
        self.output = output
        self.success_fn = success_fn
        self.hydra_task = True
        super().__init__(name=command.name, *kwargs)

    @defaults_from_attrs('command', 'input', 'output', 'success_fn')
    def run(self, command=None, input=None, output= None, success_fn=None, **kwargs):

        cmd = command(input, output, **kwargs)
        output = self.runner.run(cmd)

        if success_fn:
            success = success_fn()
            if not success:
                raise prefect.engine.signals.FAIL(f"Command {command.name} Success Function FAILED") from None
        return output


