PROTO_DIR := proto
GEN_DIR := gen
MODULE := raft 
PROJECT := raft 

PROTO_FILES := $(shell find $(PROTO_DIR) -name "*.proto")
COMPOSE = docker compose -f docker-compose.yml -p $(PROJECT)

.PHONY: gen

gen:
	protoc -I $(PROTO_DIR) \
		--go_out=. \
		--go_opt=module=$(MODULE) \
		--go-grpc_out=. \
		--go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)
