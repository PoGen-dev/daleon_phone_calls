.PHONY: up down logs test compile lint patch

up:
	docker compose up --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

test:
	pytest

compile:
	python -m compileall app

lint:
	ruff check app tests

patch:
	git diff --binary > mango-transcribe-analysis.patch
