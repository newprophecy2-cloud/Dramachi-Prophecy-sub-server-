
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux go build \
    -mod=mod \
    -ldflags="-s -w" \
    -o server main.go

FROM alpine:3.21

WORKDIR /app

COPY --from=builder /app/server ./server

ENV PORT=10000

EXPOSE 10000

CMD ["./server"]
