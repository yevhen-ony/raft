ifneq (,$(wildcard .env))
include .env
export
endif

PROTO_DIR := proto
GEN_DIR := gen
MODULE := raft 
PROJECT := raft 

RAFTD_TAG ?= raftd:local
RAFTD_DOCKERFILE := deploy/docker/raftd.Dockerfile

RAFTCTL_TAG ?= raftctl:local
RAFTCTL_DOCKERFILE := deploy/docker/raftctl.Dockerfile

PROTO_FILES := $(shell find $(PROTO_DIR) -name "*.proto")
COMPOSE = docker compose -f docker-compose.yml -p $(PROJECT)

.PHONY: gen build-raftd build-raftctl build up down logs

gen:
	protoc -I $(PROTO_DIR) \
		--go_out=. \
		--go_opt=module=$(MODULE) \
		--go-grpc_out=. \
		--go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)

build-raftd:
	docker build -f $(RAFTD_DOCKERFILE) -t $(RAFTD_TAG) .

build-raftctl:
	docker build -f $(RAFTCTL_DOCKERFILE) -t $(RAFTCTL_TAG) .

build: build-raftd build-raftctl 

up:
	$(COMPOSE) --profile cluster up -d

down:
	$(COMPOSE) --profile cluster down

logs:
	$(COMPOSE) --profile cluster logs -f

client:
	$(COMPOSE) run --rm raftctl

