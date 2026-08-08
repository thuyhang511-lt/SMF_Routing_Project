FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o load_tester_bin .

FROM alpine:latest
WORKDIR /root/

COPY --from=builder /app/load_tester_bin .

RUN chmod +x ./load_tester_bin

CMD ["./load_tester_bin"]