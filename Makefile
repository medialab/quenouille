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

lint:
	@echo Linting source code using pep8...
	pycodestyle --ignore E501,E722,E731 $(SOURCE) test
	@echo
	@echo Searching for unused imports...
	importchecker $(SOURCE) | grep -v __init__ || true
	@echo

hint:
	pylint $(SOURCE)

unit:
	@echo Running unit tests...
	pytest -sv
	@echo

upload:
	python setup.py sdist bdist_wheel
	twine upload dist/*
