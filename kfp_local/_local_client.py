__all__ = [
    "LocalClient",
    "RunPipelineResult",
]

import datetime
import json
import logging
import os
import subprocess
import tempfile
import warnings
from typing import Any, Callable, Dict, List, Mapping, Optional

import kfp
from google.protobuf import json_format, struct_pb2
from google.protobuf.descriptor import Error
from kfp.pipeline_spec import pipeline_spec_pb2
from kfp.v2.compiler import Compiler

from ._dag import Dag

OutputMetadataFilePath = "output_metadata.json"


def _get_output_file_path(
    pipeline_root: str, run_name: str, task_name: str, output_name: Optional[str] = None
) -> str:
    """Generate the local path for task output paramater file

    If the output_name is None, return the local path for the metadata json file
    """
    return os.path.join(
        pipeline_root, run_name, task_name, output_name or OutputMetadataFilePath
    )


class RunPipelineResult:
    """Information about the run result of pipeline"""

    def __init__(
        self,
        pipeline_root: str,
        run_id: str,
        success: bool,
    ):
        self._pipeline_root = pipeline_root
        self.run_id = run_id
        self._success = success

    def get_output_file(self, task_name: str, output: Optional[str] = None) -> str:
        return _get_output_file_path(
            self._pipeline_root,
            run_name=self.run_id,
            task_name=task_name,
            output_name=output,
        )

    @property
    def success(self) -> bool:
        return self._success

    def __repr__(self):
        return "RunPipelineResult(run_id={})".format(self.run_id)


class LocalClient:
    class ExecutionMode:
        """Configuration to decide whether the client executes a component in
        docker or in local process."""

        DOCKER = "docker"
        LOCAL = "local"

        def __init__(
            self,
            mode: str = DOCKER,
            images_to_exclude: List[str] = [],
            tasks_to_exclude: List[str] = [],
            docker_options: List[str] = [],
        ) -> None:
            """Constructor.

            Args:
                mode: Default execution mode, default 'docker'
                images_to_exclude: If the image of op is in images_to_exclude, the op is
                    executed in the mode different from default_mode.
                tasks_to_exclude: If the name of task is in tasks_to_exclude, the op is
                    executed in the mode different from default_mode.
                docker_options: Docker options used in docker mode,
                    e.g. docker_options=["-e", "foo=bar"].
            """
            if mode not in [self.DOCKER, self.LOCAL]:
                raise Exception("Invalid execution mode, must be docker of local")
            self._mode = mode
            self._images_to_exclude = images_to_exclude
            self._tasks_to_exclude = tasks_to_exclude
            self._docker_options = docker_options

        @property
        def mode(self) -> str:
            return self._mode

        @property
        def images_to_exclude(self) -> List[str]:
            return self._images_to_exclude

        @property
        def tasks_to_exclude(self) -> List[str]:
            return self._tasks_to_exclude

        @property
        def docker_options(self) -> List[str]:
            return self._docker_options

    def __init__(self, pipeline_root: Optional[str] = None) -> None:
        """Construct the instance of LocalClient.

        Args：
            pipeline_root: The root directory where the output artifact of component
              will be saved.
        """

        _pipeline_root = pipeline_root or tempfile.tempdir

        if _pipeline_root is None or not os.path.isdir(_pipeline_root):
            raise NotADirectoryError("pipeline_root must be local directory")

        self._pipeline_root = os.path.abspath(_pipeline_root)

    def _create_dag(self, component: pipeline_spec_pb2.ComponentSpec) -> Dag:
        """Create DAG for dag component"""
        tasks: Dict[str, pipeline_spec_pb2.PipelineTaskSpec] = component.dag.tasks
        _dag = Dag(list(tasks.keys()))
        for task_name, task in tasks.items():
            if task.dependent_tasks and len(task.dependent_tasks) > 0:
                for dependent_task in task.dependent_tasks:
                    _dag.add_edge(dependent_task, task_name)
        return _dag

    def _decode_executor_output(
        self, metadata_file: str
    ) -> pipeline_spec_pb2.ExecutorOutput:
        """Decode the executor output from the metadata file"""
        with open(metadata_file, "r") as f:
            from google.protobuf import json_format
            from kfp.pipeline_spec import pipeline_spec_pb2

            metadata = pipeline_spec_pb2.ExecutorOutput()
            json_format.Parse(f.read(), metadata)
            return metadata

    def _add_input_parameter(
        self,
        executor_input: pipeline_spec_pb2.ExecutorInput,
        param_name: str,
        param_type: pipeline_spec_pb2.PrimitiveType.PrimitiveTypeEnum,
        value,
    ):
        """Add input parameter to executor input"""
        if param_type == pipeline_spec_pb2.PrimitiveType.PrimitiveTypeEnum.INT:
            executor_input.inputs.parameters[param_name].int_value = int(value)
        elif param_type == pipeline_spec_pb2.PrimitiveType.PrimitiveTypeEnum.DOUBLE:
            executor_input.inputs.parameters[param_name].double_value = float(value)
        else:
            executor_input.inputs.parameters[param_name].string_value = str(value)

    def _resolve_inputs(
        self,
        component_input: pipeline_spec_pb2.ExecutorInput,
        upstream_tasks: Dict[str, pipeline_spec_pb2.ExecutorInput],
        task: pipeline_spec_pb2.PipelineTaskSpec,
        component: pipeline_spec_pb2.ComponentSpec,
    ) -> pipeline_spec_pb2.ExecutorInput.Inputs:
        """Resolve the inputs of task"""

        inputs = pipeline_spec_pb2.ExecutorInput.Inputs()
        for param_name, param_spec in task.inputs.parameters.items():
            kind = param_spec.WhichOneof("kind")
            if kind == "runtime_value":
                t = param_spec.runtime_value.WhichOneof("value")
                if t == "constant_value":
                    inputs.parameters[param_name].CopyFrom(
                        param_spec.runtime_value.constant_value
                    )
                elif t == "runtime_parameter":
                    raise NotImplementedError("the runtime_parameter not supported")
                else:
                    raise Error("Invalid kind of runtime value")

            elif kind == "task_output_parameter":
                task_output = param_spec.task_output_parameter
                producer_executor_input = upstream_tasks[task_output.producer_task]

                executor_output = self._decode_executor_output(
                    metadata_file=producer_executor_input.outputs.output_file
                )

                if task_output.output_parameter_key in executor_output.parameters:
                    inputs.parameters[param_name].CopyFrom(
                        executor_output.parameters[task_output.output_parameter_key]
                    )
                else:
                    output_param = producer_executor_input.outputs.parameters[
                        task_output.output_parameter_key
                    ]
                    with open(output_param.output_file, "r") as f:
                        value = f.read()
                        comp_param_spec = component.input_definitions.parameters[
                            param_name
                        ]
                        self._add_input_parameter(
                            inputs,
                            param_name=param_name,
                            param_type=comp_param_spec.type,
                            value=value,
                        )

            elif kind == "component_input_parameter":
                inputs.parameters[param_name].CopyFrom(
                    component_input.inputs.parameters[
                        param_spec.component_input_parameter
                    ]
                )
            elif kind == "task_final_status":
                raise NotImplementedError(
                    f"parameter spec of type {kind} not implemented yet"
                )
            else:
                raise Error("Invalid kind of input parameter")

        for artifact_name, artifact_spec in task.inputs.artifacts.items():

            kind = artifact_spec.WhichOneof("kind")

            if kind == "task_output_artifact":
                task_output = artifact_spec.task_output_artifact
                producer_executor_input = upstream_tasks[task_output.producer_task]

                metadata = self._decode_executor_output(
                    producer_executor_input.outputs.output_file
                )
                if task_output.output_artifact_key in metadata.artifacts:
                    output_artifact = metadata.artifacts[
                        task_output.output_artifact_key
                    ]
                else:
                    output_artifact = producer_executor_input.outputs.artifacts[
                        task_output.output_artifact_key
                    ]
                inputs.artifacts[artifact_name].CopyFrom(output_artifact)
            elif kind == "component_input_artifact":
                raise NotImplementedError(
                    f"artifact spec of type {kind} not implemented yet"
                )
            else:
                raise Error("Invalid kind of input artifact")
        return inputs

    def _provision_outputs(
        self,
        run_name: str,
        task_name: str,
        component: pipeline_spec_pb2.ComponentSpec,
    ) -> pipeline_spec_pb2.ExecutorInput.Outputs:
        """Prepare local files for the outputs"""

        outputs_spec = component.output_definitions

        outputs = pipeline_spec_pb2.ExecutorInput.Outputs(
            output_file=_get_output_file_path(
                pipeline_root=self._pipeline_root,
                run_name=run_name,
                task_name=task_name,
                output_name=OutputMetadataFilePath,
            )
        )
        for param_name, param_spec in outputs_spec.parameters.items():
            outputs.parameters[param_name].CopyFrom(
                pipeline_spec_pb2.ExecutorInput.OutputParameter(
                    output_file=_get_output_file_path(
                        pipeline_root=self._pipeline_root,
                        run_name=run_name,
                        task_name=task_name,
                        output_name=param_name,
                    )
                )
            )
        for artifact_name, artifact in outputs_spec.artifacts.items():
            outputs.artifacts[artifact_name].CopyFrom(
                pipeline_spec_pb2.ArtifactList(
                    artifacts=[
                        pipeline_spec_pb2.RuntimeArtifact(
                            uri="s3:/"
                            + _get_output_file_path(
                                pipeline_root=self._pipeline_root,
                                run_name=run_name,
                                task_name=task_name,
                                output_name=artifact_name,
                            ),
                            type=artifact.artifact_type,
                            metadata=artifact.metadata,
                        )
                    ]
                )
            )
        return outputs

    def _prepare_executor_input(
        self,
        run_name: str,
        component_input: pipeline_spec_pb2.ExecutorInput,
        upstream_tasks: Dict[str, pipeline_spec_pb2.ExecutorInput],
        task: pipeline_spec_pb2.PipelineTaskSpec,
        component: pipeline_spec_pb2.ComponentSpec,
    ) -> pipeline_spec_pb2.ExecutorInput:
        """Prepare the executor input for pipeline task"""
        return pipeline_spec_pb2.ExecutorInput(
            inputs=self._resolve_inputs(
                component_input=component_input,
                upstream_tasks=upstream_tasks,
                task=task,
                component=component,
            ),
            outputs=self._provision_outputs(
                run_name=run_name,
                task_name=task.task_info.name,
                component=component,
            ),
        )

    def _generate_cmd_for_subprocess_execution(
        self,
        container: pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec,
        executor_input: pipeline_spec_pb2.ExecutorInput,
    ) -> List[str]:
        """Generate shell command to run the op locally."""
        cmd = []
        cmd.extend(container.command)
        cmd.extend(container.args)

        for i in range(len(cmd)):
            if cmd[i] == "-c":
                # In debug mode, for `python -c cmd` format command, pydev will insert
                # code before `cmd`, but there is no newline at the end of the inserted
                # code, which will cause syntax error, so we add newline before `cmd`.
                cmd[i + 1] = "\n" + cmd[i + 1]
            elif cmd[i] == "{{$}}" and cmd[i - 1] == "--executor_input":
                cmd[i] = json_format.MessageToJson(executor_input)

        return cmd

    def _generate_cmd_for_docker_execution(
        self,
        container: pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec,
        executor_input: pipeline_spec_pb2.ExecutorInput,
        docker_options: List[str] = [],
    ) -> List[str]:
        """Generate the command to run the op in docker locally."""
        cmd = self._generate_cmd_for_subprocess_execution(
            container=container,
            executor_input=executor_input,
        )

        docker_cmd = [
            "docker",
            "run",
            *docker_options,
            "-v",
            "{pipeline_root}:{pipeline_root}".format(pipeline_root=self._pipeline_root),
            "-v",
            "{pipeline_root}:/s3{pipeline_root}".format(
                pipeline_root=self._pipeline_root
            ),
            container.image,
        ] + cmd
        return docker_cmd

    def _eval_condition(
        self, executor_input: pipeline_spec_pb2.ExecutorInput, condition: str
    ) -> bool:
        """Eval the trigger condition"""
        return eval(condition, {}, {"inputs": executor_input.inputs})

    def _run_task(
        self,
        run_name: str,
        pipeline: pipeline_spec_pb2.PipelineSpec,
        task: pipeline_spec_pb2.PipelineTaskSpec,
        executor_input: pipeline_spec_pb2.ExecutorInput,
        execution_mode: ExecutionMode,
    ) -> bool:
        """Run the pipeline task

        Args:
            run_name: unique id of the pipeline run
            pipeline: the pipeline to run
            task: the pipeline task to run
            executor_input:  the input for current task
            execution_mode: Configuration to decide whether the client executes
                component in docker or in local process.

        Returns:
            True if succeed to run the pipeline task.
        """

        current_component = pipeline.components[task.component_ref.name]
        is_container = current_component.executor_label != ""

        if is_container:
            executor = pipeline.deployment_spec["executors"][
                current_component.executor_label
            ]
            return self._run_executor_component(
                task_name=task.task_info.name,
                executor=executor,
                executor_input=executor_input,
                execution_mode=execution_mode,
            )
        else:
            return self._run_dag_component(
                run_name=run_name,
                pipeline=pipeline,
                dag_component=current_component,
                component_input=executor_input,
                execution_mode=execution_mode,
            )

    def _run_dag_component(
        self,
        run_name: str,
        pipeline: pipeline_spec_pb2.PipelineSpec,
        dag_component: pipeline_spec_pb2.ComponentSpec,
        component_input: pipeline_spec_pb2.ExecutorInput,
        execution_mode: ExecutionMode,
    ) -> bool:
        """Run tasks of current dag component in topological order.

        Args:
            run_name: unique id of the pipeline run
            pipeline: the pipeline to run
            dag_component: current dag component
            component_input: the input for current component
            execution_mode: Configuration to decide whether the client executes
                component in docker or in local process.
        Returns:
            True if succeed to run the group dag.
        """

        tasks_dag = self._create_dag(dag_component)
        executor_inputs: Dict[str, pipeline_spec_pb2.ExecutorInput] = {}

        for task_name in tasks_dag.topological_sort():
            task = dag_component.dag.tasks[task_name]
            component = pipeline.components[task.component_ref.name]
            task_executor_input = self._prepare_executor_input(
                run_name=run_name,
                component_input=component_input,
                upstream_tasks=executor_inputs,
                task=task,
                component=component,
            )
            executor_inputs[task_name] = task_executor_input

            task_success = True
            if task.trigger_policy.condition != "":
                condition = self._eval_condition(
                    task_executor_input, condition=task.trigger_policy.condition
                )
                if condition:
                    task_success = self._run_task(
                        run_name=run_name,
                        pipeline=pipeline,
                        task=task,
                        executor_input=task_executor_input,
                        execution_mode=execution_mode,
                    )
            elif task.parameter_iterator.item_input != "":
                kind = task.parameter_iterator.items.WhichOneof("kind")
                if kind == "input_parameter":
                    items_json_str = task_executor_input.inputs.parameters[
                        task.parameter_iterator.items.input_parameter
                    ].string_value
                elif kind == "raw":
                    items_json_str = task.parameter_iterator.items.raw
                else:
                    raise Error("Invalid kind of parameter iterator")

                items = json.loads(items_json_str)
                item_input = task.parameter_iterator.item_input
                for item in items:
                    task_executor_input.inputs.parameters[item_input]
                    comp_param_spec = component.input_definitions.parameters[item_input]
                    self._add_input_parameter(
                        task_executor_input,
                        param_name=item_input,
                        param_type=comp_param_spec.type,
                        value=item,
                    )

                    task_success = self._run_task(
                        run_name=run_name,
                        pipeline=pipeline,
                        task=task,
                        executor_input=task_executor_input,
                        execution_mode=execution_mode,
                    )

            elif task.artifact_iterator.item_input != "":
                # TODO support artifact iterator
                raise NotImplementedError("artifact_iterator not supported")
            else:
                task_success = self._run_task(
                    run_name=run_name,
                    pipeline=pipeline,
                    task=task,
                    executor_input=task_executor_input,
                    execution_mode=execution_mode,
                )

            if not task_success:
                return False

        return True

    def _convert_pb(self, pb_struct: struct_pb2.Struct, pb_cls) -> Any:
        """Convert generic protobuf struct to specific protobuf class"""

        pb = pb_cls()
        json_format.ParseDict(json_format.MessageToDict(pb_struct), pb)
        return pb

    def _run_container(
        self,
        task_name: str,
        container: pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec,
        executor_input: pipeline_spec_pb2.ExecutorInput,
        execution_mode: ExecutionMode,
    ) -> bool:
        """Run a single contianer execution"""

        execution_mode = (
            execution_mode if execution_mode else LocalClient.ExecutionMode()
        )
        can_run_locally = execution_mode.mode == LocalClient.ExecutionMode.LOCAL
        exclude = (
            container.image in execution_mode.images_to_exclude
            or task_name in execution_mode.tasks_to_exclude
        )
        if exclude:
            can_run_locally = not can_run_locally

        if can_run_locally:
            cmd = self._generate_cmd_for_subprocess_execution(
                container=container,
                executor_input=executor_input,
            )
        else:
            cmd = self._generate_cmd_for_docker_execution(
                container=container,
                executor_input=executor_input,
                docker_options=execution_mode.docker_options,
            )

        process = subprocess.Popen(
            cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        # TODO support async process
        logging.info("start task：%s", task_name)
        stdout, stderr = process.communicate()
        if stdout:
            logging.info(stdout)
        if stderr:
            logging.error(stderr)
        if process.returncode != 0:
            logging.error(cmd)
            return False
        return True

    def _run_executor_component(
        self,
        task_name: str,
        executor: pipeline_spec_pb2.PipelineDeploymentConfig,
        executor_input: pipeline_spec_pb2.ExecutorInput,
        execution_mode: ExecutionMode,
    ) -> bool:
        """Run a single execution"""

        if executor["container"]:
            container = self._convert_pb(
                executor["container"],
                pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec,
            )
            return self._run_container(
                task_name=task_name,
                container=container,
                executor_input=executor_input,
                execution_mode=execution_mode,
            )
        elif executor["importer"]:
            raise NotImplementedError("Importer not supported")
        else:
            raise Error("Unknown type of executor")

    def create_run_from_pipeline_func(
        self,
        pipeline_func: Callable,
        arguments: Mapping[str, str],
        execution_mode: ExecutionMode = ExecutionMode(),
        output_path: Optional[str] = None,
    ):
        """Runs a pipeline locally, either using Docker or in a local process.

        Parameters:
          pipeline_func: pipeline function
          arguments: Arguments to the pipeline function provided as a dict, reference
              to `kfp.client.create_run_from_pipeline_func`
          execution_mode: Configuration to decide whether the client executes component
              in docker or in local process.
          output_path: The file path to be written.
        """

        compiling_for_v2_old_value = kfp.COMPILING_FOR_V2
        try:
            compiler = Compiler()
            pipeline_job = compiler._create_pipeline_v2(
                pipeline_func=pipeline_func, pipeline_parameters_override=arguments
            )
            if output_path:
                compiler._write_pipeline(pipeline_job, output_path)
        finally:
            kfp.COMPILING_FOR_V2 = compiling_for_v2_old_value

        pipeline_spec = pipeline_spec_pb2.PipelineSpec()
        json_format.ParseDict(
            json_format.MessageToDict(pipeline_job.pipeline_spec), pipeline_spec
        )

        run_version = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        run_name = (
            pipeline_spec.pipeline_info.name.replace(" ", "_").lower()
            + "_"
            + run_version
        )
        root_component_input = pipeline_spec_pb2.ExecutorInput(
            inputs=pipeline_spec_pb2.ExecutorInput.Inputs(
                parameters=pipeline_job.runtime_config.parameters
            )
        )

        success = self._run_dag_component(
            run_name,
            pipeline=pipeline_spec,
            dag_component=pipeline_spec.root,
            component_input=root_component_input,
            execution_mode=execution_mode,
        )

        return RunPipelineResult(self._pipeline_root, run_name, success=success)
