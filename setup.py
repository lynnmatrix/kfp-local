import os
from typing import Dict, List

from setuptools import find_packages, setup

ROOT_PATH = os.path.dirname(__file__)
PKG_NAME = "kfp-local"


def _load_version() -> str:
    version = ""
    version_path = os.path.join(ROOT_PATH, "kfp_local", "_version.py")
    with open(version_path) as fp:
        version_module: Dict[str, str] = {}
        exec(fp.read(), version_module)
        version = version_module["__version__"]

    return version


def _load_description() -> str:
    readme_path = os.path.join(ROOT_PATH, "README.md")
    with open(readme_path) as fp:
        return fp.read()


def _load_requires(
    requirements="requirements.in",
) -> List[str]:
    requirements_path = os.path.join(ROOT_PATH, requirements)
    with open(requirements_path) as f:
        lines = list(
            filter(
                lambda l: not l.startswith("#"), map(lambda l: l.strip(), f.readlines())
            )
        )

    return lines


setup(
    name=PKG_NAME,
    version=_load_version(),
    install_requires=_load_requires(),
    python_requires=">=3.7",
    packages=find_packages(),
    include_package_data=True,
    description="kfp local client runs pipelines on local host or in docker container",
    long_description=_load_description(),
    long_description_content_type="text/markdown",
    keywords="kubeflow, kfp, local",
)
