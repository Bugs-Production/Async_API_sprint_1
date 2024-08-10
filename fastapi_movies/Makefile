.PHONY: build
build:
	 docker-compose build

.PHONY: up
up:
	 docker-compose up -d

.PHONY: down
down:
	 docker-compose down

.PHONY: docker_test
docker_test:
	docker exec -it movies_tests pytest tests/functional/src

.PHONY: format
format:
	black . && isort .
