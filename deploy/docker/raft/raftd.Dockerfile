FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/raftd \
    ./cmd/raftd

FROM alpine:3.22

COPY --from=build /out/raftd /usr/local/bin/raftd
COPY cmd/raftd/config.yml /etc/raft/config.yml

EXPOSE 5001

ENTRYPOINT ["raftd"]
CMD ["--config", "/etc/raft/config.yml"]
