# kfp-local
Pipelines built with [kfp(kubeflow pipeline)](https://github.com/kubeflow/pipelines) execute in k8s cluster.

During the development of kubeflow pipeline components, we usually upload the pipeline to kfp server and run it in k8s, if anything wrong we fix it locally and upload it again.

It should be helpful for the development efficiency if the above process takes place in local host, that's what `kfp-local` cares.

## Installation
```
pip install kfp-local
```

## Usage
```python
from kfp_local import LocalClient

local_client = LocalClient()
result = local_client.create_run_from_pipeline_func(
            pipeline_func,
            arguments={"foo": "bar"},
            execution_mode=LocalClient.ExecutionMode("local"),
        )
if result.success:
    a_output_filepath = result.get_output_file(task_name="a-task", output="a_output_name")

```

## Additional configuration

The demo code in [Usage](#Usage) executes pipeline in local process with *ExecutionMode('local')*, `ExecutionMode` controls how the pipline executes.

There are some options of `ExecutionMode`:
* mode: Default execution mode, default 'docker'

* images_to_exclude: If the image of op is in images_to_exclude, the op is
    executed in the mode different from default_mode.

* tasks_to_exclude: If the name of task is in tasks_to_exclude, the op is executed in the mode different from default_mode.

* docker_options: Docker options used in docker mode, e.g. docker_options=["-e", "foo=bar"].

For more information about how to make use of kfp_local, please refer to unit test.

## kfp compatibility
kfp-local is tested with kfp>=1.8.9,<2.0 for now.

Supports:
* Control flow: Condition, ParallelFor, ExitHandler
* Data passing: InputPath, OutputPath, Input

Don't support for now:
* Importer
* Artifact iterator
* Caching
