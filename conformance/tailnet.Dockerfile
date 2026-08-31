# syntax=docker/dockerfile:1
FROM golang:1.26-bookworm AS build

WORKDIR /src
COPY . .
RUN go build -trimpath -o /out/vgi-rpc-tailnet-go ./conformance/cmd/vgi-rpc-tailnet-go

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/vgi-rpc-tailnet-go /usr/local/bin/vgi-rpc-tailnet-go
ENTRYPOINT ["vgi-rpc-tailnet-go"]
