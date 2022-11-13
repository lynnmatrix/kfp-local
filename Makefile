.PHONY: requirements install-dev build test lint fmt version clean

requirements: requirements-dev.txt requirements.txt

requirements-dev.txt: requirements-dev.in requirements.in
	pip-compile --verbose requirements-dev.in -o requirements-dev.txt --resolver backtracking

requirements.txt: requirements.in
	pip-compile --verbose --resolver backtracking

install-dev: requirements
	pip install -r requirements-dev.txt
	pip install --editable .

build: requirements
	python setup.py sdist bdist_wheel

clean:
	rm -rf ./dist
	rm -rf ./build

pushtest: clean build
	twine upload --repository testpypi  dist/*

push: clean build
	twine upload --repository pypi  dist/*

lint: fmt
	flake8 .
	mypy .
	# Check if something has changed after generation
	git \
		--no-pager diff \
		--exit-code \
		.

fmt:
	black .
