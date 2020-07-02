.PHONY: clean-pyc clean-build docs clean 

JUNIT := "tests/junit/results.xml"

help:
	@echo "clean - test, coverage and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage quickly with the default Python"

clean: clean-pyc clean-test

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr tests/junit/
	rm -fr tmp
	rm -rf .cache

lint:
	flake8 charm_product schema tests setup.py

test:
	pytest tests --junitxml $(JUNIT)

coverage:
	coverage erase
	coverage run --source charm_product -m pytest tests --junitxml $(JUNIT) -vvv
	coverage report --include=charm_product/*
	coverage html --fail-under=60
