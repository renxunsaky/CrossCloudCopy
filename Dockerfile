##
## Build
##
FROM golang:1.17-alpine as build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o ./C3


##
## Deploy
##
FROM alpine:3.15
WORKDIR /

COPY --from=build /app/C3 /usr/bin/

EXPOSE 9000
ENTRYPOINT ["/usr/bin/C3"]