# Variables
SOURCE = quenouille

# Commands
all: lint test
test: unit
publish: clean lint test upload
	$(call clean)

define clean
	rm -rf *.egg-info .pytest_cache build dist
	find . -name "*.pyc" | xargs rm
	find . -name __pycache__ | xargs rm -rf
endef

clean:
	$(call clean)

deps:
	pip3 install -U pip
	pip3 install -r requirements.txt

format:
	@echo Formatting source code using black
	black $(SOURCE) ftest test setup.py -t py35
	@echo

lint:
	@echo Searching for unused imports...
	importchecker $(SOURCE) | grep -v __init__ || true
	importchecker test | grep -v __init__ || true
	@echo

hint:
	pylint $(SOURCE)

unit:
	@echo Running unit tests...
	pytest -svvv
	@echo

upload:
	python setup.py sdist bdist_wheel
	twine upload dist/*
