requirements:
	pip install -r requirements_dev.txt
	pip-compile --output-file requirements.txt requirements.in
	pip install -r requirements.txt


black: ## run formatter with black
	black --line-length 100 --skip-string-normalization src test

lint: ## check style with flake8
	black --line-length 100 --skip-string-normalization --check src test
    flake8 src test


install: clean ## install the package to the active Python's site-packages
	python setup.py install



