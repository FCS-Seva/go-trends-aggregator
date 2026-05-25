FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/trends ./cmd/trends
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/loadgen ./cmd/loadgen

FROM alpine:3.20 AS trends
RUN adduser -D -H app
USER app
COPY --from=build /out/trends /usr/local/bin/trends
EXPOSE 8080
CMD ["trends"]

FROM alpine:3.20 AS loadgen
RUN adduser -D -H app
USER app
COPY --from=build /out/loadgen /usr/local/bin/loadgen
CMD ["loadgen"]
