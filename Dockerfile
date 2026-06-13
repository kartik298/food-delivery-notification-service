FROM golang:1.22-alpine
WORKDIR /app
COPY . .
RUN go mod tidy && go build -o service .
EXPOSE 8084
CMD ["./service"]
