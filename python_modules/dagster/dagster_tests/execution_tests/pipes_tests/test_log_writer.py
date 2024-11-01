import shutil
from typing import Iterator

import pytest
from dagster import AssetExecutionContext, DagsterInstance, asset, materialize
from dagster._core.pipes.subprocess import PipesSubprocessClient
from dagster._core.pipes.utils import PipesTempFileMessageReader

from dagster_tests.execution_tests.pipes_tests.utils import temp_script

_PYTHON_EXECUTABLE = shutil.which("python")


@pytest.fixture
def external_script_file_log_writer() -> Iterator[str]:
    # This is called in an external process and so cannot access outer scope
    def script_fn():
        import os
        import sys

        from dagster_pipes import (
            DAGSTER_PIPES_LOG_WRITER_KEY,
            PipesEnvVarParamsLoader,
            PipesOutErrFileLogWriter,
            open_dagster_pipes,
        )

        with open_dagster_pipes(log_writer=PipesOutErrFileLogWriter()):
            print("Writing this to stdout")  # noqa: T201
            print("And this to stderr", file=sys.stderr)  # noqa: T201

            logs_dir = PipesEnvVarParamsLoader().load_messages_params()[
                DAGSTER_PIPES_LOG_WRITER_KEY
            ][PipesOutErrFileLogWriter.LOGS_DIR_KEY]

        assert set(os.listdir(logs_dir)) == {"stderr", "stdout"}

        with open(os.path.join(logs_dir, "stdout"), "r") as stdout_file:
            contents = stdout_file.read()
            assert "Writing this to stdout" in contents

        with open(os.path.join(logs_dir, "stderr"), "r") as stderr_file:
            contents = stderr_file.read()
            assert "And this to stderr" in contents

    with temp_script(script_fn) as script_path:
        yield script_path


# in this test we do not check log reading logic in Dagster
# only that the log writer is correctly configured and launched in the remote process
def test_pipes_file_log_writer(
    capsys,
    tmpdir,
    external_script_file_log_writer,
):
    message_reader = PipesTempFileMessageReader(enable_log_writer=True)

    @asset
    def foo(context: AssetExecutionContext, ext: PipesSubprocessClient):
        cmd = [_PYTHON_EXECUTABLE, external_script_file_log_writer]
        return ext.run(
            command=cmd,
            context=context,
        ).get_results()

    resource = PipesSubprocessClient(message_reader=message_reader)

    with capsys.disabled(), DagsterInstance.ephemeral() as instance:
        result = materialize([foo], instance=instance, resources={"ext": resource})
        assert result.success
