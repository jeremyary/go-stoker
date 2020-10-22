#
# Copyright Strimzi authors.
# License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
#

# Changing go version of image should reflect in go.mod
FROM golang:1.14-alpine

RUN mkdir /app
ADD . /app
WORKDIR /app

RUN go mod download

RUN go build -o main .

CMD ["/app/main"]