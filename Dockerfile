FROM postgres:9.6-alpine

RUN wget https://github.com/2ndQuadrant/pglogical/archive/REL1_2_2.tar.gz && \
    tar -zxvf REL1_2_2.tar.gz && \
    apk add --no-cache build-base libxslt-dev libxml2-dev openssl-dev libedit-dev zlib-dev && \
    cd /pglogical-REL1_2_2 && \
    make clean all && \
    make install && \
    cd / && \
    rm -rf /REL1_2_2.tar.gz /pglogical-REL1_2_2
