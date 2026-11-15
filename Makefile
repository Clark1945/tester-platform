# 薄包裝；Windows 沒有 make 時可直接執行右邊的指令（go 指令可改用 scripts/go.ps1）
DATABASE_URL ?= postgres://tester:tester@localhost:5440/tester
AMQP_URL     ?= amqp://tester:tester@localhost:5673/

.PHONY: up down reset test k6 chaos chaos-full

up:
	docker compose up -d --build

down:
	docker compose down

reset:
	docker compose down -v

test:
	DATABASE_URL=$(DATABASE_URL) AMQP_URL=$(AMQP_URL) go test -race ./...

k6:
	k6 run loadtest/k6.js

# 一鍵 chaos test：流量進行中停 receiver、停 postgres，結束後自動對帳（失敗時 exit code 非 0）
chaos: up
	docker compose --profile tools run --rm --build chaos

# 另外再重啟 RabbitMQ
chaos-full: up
	docker compose --profile tools run --rm --build chaos -duration 90s -rabbitmq
