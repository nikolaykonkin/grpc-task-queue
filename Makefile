.PHONY: proto

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