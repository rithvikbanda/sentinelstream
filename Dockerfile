# --- Build stage ---
FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod ./
COPY . .

# CGO_ENABLED=0 produces static binaries with no libc dependency, so the
# final image can be a plain Alpine base with no Go toolchain in it.
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -o /out/simulator ./cmd/simulator \
 && CGO_ENABLED=0 go build -o /out/replay ./cmd/replay

# --- Runtime stage ---
FROM alpine:3.20

RUN adduser -D -H sentinel
WORKDIR /app

COPY --from=builder /out/server /app/server
COPY --from=builder /out/simulator /app/simulator
COPY --from=builder /out/replay /app/replay

USER sentinel

EXPOSE 9000/udp 9001/tcp 8080/tcp

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O- http://localhost:8080/api/v1/metrics/summary || exit 1

ENTRYPOINT ["/app/server"]
