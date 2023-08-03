FROM gcr.io/distroless/base-debian10

COPY bin/out /

ENTRYPOINT ["/pgcapture"]
