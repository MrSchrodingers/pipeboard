.PHONY: lint type test quick

lint:
	ruff check .

type:
	mypy .

test:
	pytest -q

quick: lint type
	pytest --collect-only -q
