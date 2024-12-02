FROM golang:latest

WORKDIR /build

# Copy the Go module files
COPY go.mod .
COPY go.sum .

# Download the Go module dependencies
RUN go mod download

COPY . .

RUN go build -o /app .

CMD ["/app"]