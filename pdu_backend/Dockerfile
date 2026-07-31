FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o pdu_app .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/pdu_app .

EXPOSE 8080
ENTRYPOINT ["./pdu_app"]