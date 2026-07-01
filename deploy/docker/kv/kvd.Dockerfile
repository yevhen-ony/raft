FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/kvd \
    ./cmd/kvd

FROM alpine:3.22

COPY --from=build /out/kvd /usr/local/bin/kvd
COPY cmd/kvd/config.yml /etc/kv/config.yml

EXPOSE 5001

ENTRYPOINT ["kvd"]
CMD ["--config", "/etc/kv/config.yml"]
