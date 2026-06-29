FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/raftctl \
    ./cmd/raftctl

FROM alpine:3.22

COPY --from=build /out/raftctl /usr/local/bin/raftctl

CMD ["/bin/sh"]
