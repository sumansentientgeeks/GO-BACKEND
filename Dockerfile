FROM golang:1.26

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /app/server ./cmd/server
RUN chmod +x /app/server /app/start.sh

EXPOSE 8080

CMD ["/app/start.sh"]