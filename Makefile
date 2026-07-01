ifneq (,$(wildcard .env))
include .env
export
endif

PROTO_DIR := proto
GEN_DIR := gen
MODULE := raft 
PROJECT := raft

RAFTD_TAG ?= raftd:local
RAFTD_DOCKERFILE := deploy/docker/raft/raftd.Dockerfile
RAFTCTL_TAG ?= raftctl:local
RAFTCTL_DOCKERFILE := deploy/docker/raft/raftctl.Dockerfile
RAFT_COMPOSE_FILE := deploy/docker-compose/raft/docker-compose.yml

KVD_TAG ?= kvd:local
KVD_DOCKERFILE := deploy/docker/kv/kvd.Dockerfile
KVCLI_TAG ?= kvcli:local
KVCLI_DOCKERFILE := deploy/docker/kv/kvcli.Dockerfile
KV_COMPOSE_FILE := deploy/docker-compose/kv/docker-compose.yml

PROTO_FILES := $(shell find $(PROTO_DIR) -name "*.proto")

RAFT_COMPOSE = docker compose -f $(RAFT_COMPOSE_FILE) -p $(PROJECT)-raft
KV_COMPOSE = docker compose -f $(KV_COMPOSE_FILE) -p $(PROJECT)-kv

.PHONY: gen
.PHONY: build-raftd build-raftctl build-raft up-raft down-raft logs-raft client-raft
.PHONY: build-kvd build-kvcli build-kv up-kv down-kv logs-kv client-kv

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

build-raft: build-raftd build-raftctl 

up-raft:
	$(RAFT_COMPOSE) --profile cluster up -d

down-raft:
	$(RAFT_COMPOSE) --profile cluster down

logs-raft:
	$(RAFT_COMPOSE) --profile cluster logs -f

client-raft:
	$(RAFT_COMPOSE) run --rm raftctl

build-kvd:
	docker build -f $(KVD_DOCKERFILE) -t $(KVD_TAG) .

build-kvcli:
	docker build -f $(KVCLI_DOCKERFILE) -t $(KVCLI_TAG) .

build-kv: build-kvd build-kvcli 

up-kv:
	$(KV_COMPOSE) --profile cluster up -d

down-kv:
	$(KV_COMPOSE) --profile cluster down

logs-kv:
	$(KV_COMPOSE) --profile cluster logs -f

client-kv:
	$(KV_COMPOSE) run --rm kvcli 
