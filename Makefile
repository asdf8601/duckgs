venv := .venv
python := $(venv)/bin/python
pip := $(python) -m pip

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  help     Show this help"
	@echo "  dev      Install dependencies in development mode"
	@echo "  release  Bump version and create a git tag"
	@echo "  publish  Push changes and tags to origin"

$(venv):
	if [ -x $$(command -v conda) ]; then conda create --yes --prefix $(venv) python=3.11; else python3.11 -m venv $(venv); fi

release:
	sed -i 's/version = ".*"/version = "$(v)"/' pyproject.toml
	git add pyproject.toml
	git commit -m "Bump version $(v)"
	git tag -a $(v) -m "Release $(v)"

publish:
	git push origin main
	git push --tags

dev: $(venv)
	$(pip) install -e .
