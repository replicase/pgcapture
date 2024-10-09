FROM postgres:17-alpine

RUN wget https://github.com/2ndQuadrant/pglogical/archive/REL2_4_5.tar.gz && \
    tar -zxvf REL2_4_5.tar.gz && \
    apk -U add --no-cache build-base libxslt-dev libxml2-dev openssl-dev libedit-dev zlib-dev lz4-dev patch $DOCKER_PG_LLVM_DEPS krb5-pkinit krb5-dev krb5

COPY 17/pglogical/pglogical.patch ./pglogical-REL2_4_5

RUN cd pglogical-REL2_4_5 && \
    patch -p1 < pglogical.patch && \
    make USE_PGXS=1 CPPFLAGS="-DPGL_NO_STDIN_ASSIGN" clean all && \
    make install && \
    cd / && \
    rm -rf /REL2_4_5.tar.gz.tar.gz /pglogical-REL2_4_5.tar.gz

RUN chown -R postgres:postgres /usr/local/share/postgresql/extension
RUN chown -R postgres:postgres /usr/local/lib/postgresql

COPY ./extension /extension
RUN cd /extension && ./make.sh
