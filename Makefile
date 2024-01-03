.PHONY: help setup test clean release format install docker run

.DEFAULT_GOAL = help

TAG:=dev$(shell echo `date '+%Y%m%d%H%M%S'`)

help:
	@echo "---------------HELP-----------------"
	@echo "To setup the project type make install"
	@echo "To test the project type make test"
	@echo "To run the project type make run"
	@echo "------------------------------------"

format:
	pre-commit run --all-files

install:
	pip install -r requirements-dev.txt
	pip install -r requirements.txt

test:
	pytest tests

clean:
	rm -rf .pytest_cache
	rm -rf __pycache__
	rm -rf .mypy_cache/

run:
	@echo -n "Building docker image with tag $(TAG)"
	docker build -f Dockerfile --platform linux/x86_64 -t eu.ovh.io/ademe-data/ademe_pipeline:$(TAG) .
	@echo -n "Running docker image with tag $(TAG)"
	docker run -v ./output:/ROOT/output eu.ovh.io/ademe-data/ademe_pipeline:$(TAG)
