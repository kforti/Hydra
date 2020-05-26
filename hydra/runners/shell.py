import logging
import os
import tempfile
import datetime
from subprocess import PIPE, STDOUT, Popen
from typing import Any

import prefect


class ShellRunner:
    """
    class for running arbitrary shell commands.

    Original Source: Prefect.tasks.shell.ShellTask
    Args:
        - command (string, optional): shell command to be executed; can also be
            provided post-initialization by calling this task instance
        - env (dict, optional): dictionary of environment variables to use for
            the subprocess; can also be provided at runtime
        - helper_script (str, optional): a string representing a shell script, which
            will be executed prior to the `command` in the same process. Can be used to
            change directories, define helper functions, etc. when re-using this Task
            for different commands in a Flow
        - shell (string, optional): shell to run the command with; defaults to "bash"
        - return_all (bool, optional): boolean specifying whether this task should return all lines of stdout
            as a list, or just the last line as a string; defaults to `False`
        - **kwargs: additional keyword arguments to pass to the Task constructor
    Example:
        ```python
        from prefect import Flow
        from prefect.tasks.shell import ShellTask
        task = ShellTask(helper_script="cd ~")
        with Flow("My Flow") as f:
            # both tasks will be executed in home directory
            contents = task(command='ls')
            mv_file = task(command='mv .vimrc /.vimrc')
        out = f.run()
        ```
    """

    def __init__(
        self,
        env: dict = None,
        helper_script: str = None,
        shell: str = "bash",
        return_all: bool = True
    ):
        self.env = env
        self.helper_script = helper_script
        self.shell = shell
        self.return_all = return_all
        self.logger = logging.getLogger()

    def run(self, command):
        """
        Run the shell command.
        Args:
            - command (string): shell command to be executed; can also be
                provided at task initialization. Any variables / functions defined in
                `self.helper_script` will be available in the same process this command
                runs in
            - env (dict, optional): dictionary of environment variables to use for
                the subprocess
        Returns:
            - stdout (string): if `return_all` is `False` (the default), only the last line of stdout
                is returned, otherwise all lines are returned, which is useful for passing
                result of shell command to other downstream tasks. If there is no output, `None` is returned.
        Raises:
            - prefect.engine.signals.FAIL: if command has an exit code other
                than 0
        """

        current_env = os.environ.copy()
        current_env.update(self.env or {})
        with tempfile.NamedTemporaryFile(prefix="hydra-") as tmp:
            if self.helper_script:
                tmp.write(self.helper_script.encode())
                tmp.write("\n".encode())
            tmp.write(command.encode())
            tmp.flush()
            start = datetime.datetime.now()
            sub_process = Popen(
                [self.shell, tmp.name], stdout=PIPE, stderr=STDOUT, env=current_env
            )

            lines = []
            line = None
            for raw_line in iter(sub_process.stdout.readline, b""):
                line = raw_line.decode("utf-8").rstrip()
                if self.return_all:
                    lines.append(line)
                else:
                    pass
                    # if we're returning all, we don't log every line
                    #self.logger.debug(line)
            sub_process.wait()
            finish = datetime.datetime.now()
            if sub_process.returncode:
                msg = "Command failed with exit code {0}: {1}".format(
                    sub_process.returncode, line
                )
                self.logger.error(msg)
                raise prefect.engine.signals.FAIL(msg) from None  # type: ignore
        if self.return_all:
            return lines
        else:
            return line

