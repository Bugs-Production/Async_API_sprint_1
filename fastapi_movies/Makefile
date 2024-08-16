.PHONY: build
build:
	docker-compose build

.PHONY: up
up:
	docker-compose up -d

.PHONY: down
down:
	docker-compose down -v

.PHONY: build_test
build_test:
	docker-compose -f docker-compose.test.yml build

.PHONY: up_test
up_test:
	docker-compose down -v
	docker-compose -f docker-compose.test.yml up -d

.PHONY: down_test
down_test:
	docker-compose -f docker-compose.test.yml down -v

.PHONY: test
test:
	docker exec -it movies_tests pytest tests/functional/src -s

.PHONY: format
format:
	black . && isort .
