import unittest
from typing import Union

from kfp.v2 import dsl

from kfp_local import LocalClient


@dsl.component
def hello(name: str) -> str:
    return f"hello {name}"


@dsl.component
def local_loader(src: str, dst: dsl.OutputPath(str)):
    import os
    import shutil

    if os.path.exists(src):
        shutil.copyfile(src, dst)


@dsl.component
def flip_coin() -> str:
    import random

    return "head" if random.randint(0, 1) == 0 else "tail"


@dsl.component
def component_with_inputpath(src: dsl.InputPath()) -> str:
    with open(src, "r") as f:
        return f.read()


@dsl.component
def component_return_artifact(content: str) -> dsl.Artifact:
    return content


@dsl.component
def component_consume_artifact(artifact: dsl.Input[dsl.Artifact]) -> str:
    with open(artifact.path, "r") as f:
        return f.read()


class LocalRunnerTest(unittest.TestCase):
    def setUp(self):
        self.local_client = LocalClient()
        import tempfile

        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            self.temp_file_path = f.name
            f.write("hello world")

    def get_param_output(self, result, task_name: str, output_name: str = None) -> str:
        with open(result.get_output_file(task_name, output_name), "r") as f:
            return f.read()

    def get_param_return(self, result, task_name: str, output_name: str = None):
        with open(result.get_output_file(task_name, "output_metadata.json"), "r") as f:
            from google.protobuf import json_format
            from kfp.pipeline_spec import pipeline_spec_pb2

            metadata = pipeline_spec_pb2.ExecutorOutput()
            json_format.Parse(f.read(), metadata)
            output = metadata.parameters[output_name or "Output"]
            value_type = output.WhichOneof("value")
            if value_type == "string_value":
                output_value = output.string_value
            elif value_type == "int_value":
                output_value = output.int_value
            elif value_type == "double_value":
                output_value = output.double_value
            else:
                output_value = None
            return output_value

    def assert_func_param_return_equal(
        self,
        content: Union[str, int, float, bool, dict, list],
        result,
        task_name: str,
        output_name: str = None,
    ):
        output_value = self.get_param_return(result, task_name, output_name)

        if type(output_value) == str and type(content) != str:
            import json

            content = json.dumps(content)

        self.assertEqual(content, output_value)

    def test_run_local(self):
        @dsl.pipeline(name="test-run-local-pipeline")
        def _pipeline(name: str):
            hello(name)

        result = self.local_client.create_run_from_pipeline_func(
            _pipeline,
            {"name": "world"},
            execution_mode=LocalClient.ExecutionMode("local"),
        )
        self.assertTrue(result.success)
        self.assert_func_param_return_equal("hello world", result, "hello")

    def test_local_file(self):
        @dsl.pipeline(name="test-local-file-pipeline")
        def _pipeline(file_path: str):
            local_loader(file_path)

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline,
            {"file_path": self.temp_file_path},
            execution_mode=LocalClient.ExecutionMode("local"),
        )
        self.assertEqual(
            "hello world", self.get_param_output(run_result, "local-loader", "dst")
        )

    def test_condition(self):
        @dsl.pipeline(name="test-condition")
        def _pipeline():
            _flip = flip_coin()
            with dsl.Condition(_flip.output == "head"):
                hello("head")

            with dsl.Condition(_flip.output == "tail"):
                hello("tail")

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline, {}, execution_mode=LocalClient.ExecutionMode("local")
        )
        self.assertTrue(run_result.success)
        coin_result = self.get_param_return(run_result, "flip-coin")
        if coin_result == "head":
            self.assert_func_param_return_equal("hello head", run_result, "hello")
        else:
            self.assert_func_param_return_equal("hello tail", run_result, "hello-2")

    def test_loop(self):
        @dsl.pipeline(name="for-pipeline")
        def _pipeline():
            with dsl.ParallelFor(["hello", "world"]) as item:
                hello(item)

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline, {}, execution_mode=LocalClient.ExecutionMode("local")
        )
        self.assertTrue(run_result.success)
        self.assertEqual(
            "hello world", self.get_param_return(result=run_result, task_name="hello")
        )

    def test_nest_loop(self):
        @dsl.pipeline(name="test-nest-loop")
        def _pipeline():
            loop_parameter = [[1, 2], [3, 4]]
            with dsl.ParallelFor(loop_parameter) as item:
                with dsl.ParallelFor(item) as item_a:
                    hello(item)
                    hello(item_a)

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline, {}, execution_mode=LocalClient.ExecutionMode("local")
        )
        self.assertTrue(run_result.success)
        self.assertEqual("hello 4", self.get_param_return(run_result, "hello-2"))

    def test_exit_handler(self):
        @dsl.pipeline("test-exit-handler")
        def _pipeline():
            with dsl.ExitHandler(exit_op=hello("exit")):
                hello("inner")

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline, {}, execution_mode=LocalClient.ExecutionMode("local")
        )

        self.assertTrue(run_result.success)
        self.assertEqual("hello exit", self.get_param_return(run_result, "hello"))
        self.assertEqual("hello inner", self.get_param_return(run_result, "hello-2"))

    @unittest.skip("docker is not installed in CI environment.")
    def test_connect_artifact(self):
        @dsl.pipeline(name="test-connect-artifact")
        def _pipeline():
            input_component = component_return_artifact("hello world")
            component_with_inputpath(input_component.output)

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline,
            {},
            execution_mode=LocalClient.ExecutionMode("docker"),
        )
        self.assertTrue(run_result.success)
        self.assertEqual(
            "hello world",
            self.get_param_return(run_result, "component-with-inputpath"),
        )

    @unittest.skip("docker is not installed in CI environment.")
    def test_output_artifact(self):
        @dsl.pipeline(name="test-output-artifact")
        def _pipeline():
            component1 = component_return_artifact("artifact content")
            component_consume_artifact(component1.output)

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline, {}, execution_mode=LocalClient.ExecutionMode("docker")
        )
        self.assertTrue(run_result.success)
        self.assertEqual(
            "artifact content",
            self.get_param_return(run_result, "component-consume-artifact"),
        )

    @unittest.skip("docker is not installed in CI environment.")
    def test_execution_mode_exclude_op(self):
        @dsl.component(base_image="image_not_exist")
        def cat_on_image_not_exist(name: str) -> str:
            return name

        @dsl.pipeline(name="test-execution-mode-exclude-op")
        def _pipeline():
            cat_on_image_not_exist("exclude tasks")

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline,
            {},
            execution_mode=LocalClient.ExecutionMode(mode="docker"),
        )
        self.assertFalse(run_result.success)

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline,
            {},
            execution_mode=LocalClient.ExecutionMode(
                mode="docker", tasks_to_exclude=["cat-on-image-not-exist"]
            ),
        )
        self.assertTrue(run_result.success)
        self.assertEqual(
            "exclude tasks",
            self.get_param_return(run_result, "cat-on-image-not-exist"),
        )

    @unittest.skip("docker is not installed in CI environment.")
    def test_docker_options(self):
        @dsl.component
        def check_option(env_name: str) -> str:
            import os

            return os.environ[env_name]

        @dsl.pipeline(name="test-docker-options")
        def _pipeline():
            check_option("foo")

        run_result = self.local_client.create_run_from_pipeline_func(
            _pipeline,
            {},
            execution_mode=LocalClient.ExecutionMode(
                mode="docker", docker_options=["-e", "foo=bar"]
            ),
        )
        assert run_result.success
        self.assertEqual("bar", self.get_param_return(run_result, "check-option"))
