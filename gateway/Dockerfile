FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o gateway_app .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/gateway_app .

EXPOSE 8000
CMD ["./gateway_app"]