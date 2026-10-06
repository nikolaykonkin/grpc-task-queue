.PHONY: proto build run-api run-worker test test-race vet

# Все proto-файлы проекта
PROTO_FILES := proto/task/v1/task.proto

# Генерация Go-кода из proto-контракта
# --proto_path=proto: корень, от которого считаются пути импорта
# paths=source_relative: путь в gen/ повторяет путь в proto/
proto:
	@mkdir -p gen
	protoc \
		--proto_path=proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

# Сборка обоих бинарников в bin/
build:
	@mkdir -p bin
	go build -o bin/task-api ./cmd/task-api
	go build -o bin/task-worker ./cmd/task-worker

# Запуск сервера
run-api:
	go run ./cmd/task-api

# Запуск воркера
run-worker:
	go run ./cmd/task-worker

# Обычный запуск тестов
test:
	go test ./...

# Тесты с детектором гонок данных
test-race:
	go test -race ./...

# Статический анализ
vet:
	go vet ./...