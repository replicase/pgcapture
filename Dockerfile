FROM golang:1.16 AS base

WORKDIR /src

ADD go.* .
RUN go mod download

ADD . .

ARG SHA
RUN go build -ldflags="-X github.com/rueian/pgcapture/cmd.CommitSHA=${SHA}" -x -o pgcapture main.go

FROM gcr.io/distroless/base-debian10

COPY --from=base /src/pgcapture /pgcapture

ENTRYPOINT ["/pgcapture"]