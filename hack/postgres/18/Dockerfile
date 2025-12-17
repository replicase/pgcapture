FROM postgres:18-alpine

RUN wget https://github.com/2ndQuadrant/pglogical/archive/REL2_4_6.tar.gz && \
    tar -zxvf REL2_4_6.tar.gz && \
    apk -U add --no-cache build-base libxslt-dev libxml2-dev openssl-dev libedit-dev zlib-dev lz4-dev patch $DOCKER_PG_LLVM_DEPS krb5-pkinit krb5-dev krb5

COPY 18/pglogical/pglogical.patch ./pglogical-REL2_4_6

RUN cd pglogical-REL2_4_6 && \
    patch -p1 < pglogical.patch && \
    make USE_PGXS=1 CPPFLAGS="-DPGL_NO_STDIN_ASSIGN" clean all && \
    make install && \
    cd / && \
    rm -rf /REL2_4_6.tar.gz /pglogical-REL2_4_6

RUN chown -R postgres:postgres /usr/local/share/postgresql/extension
RUN chown -R postgres:postgres /usr/local/lib/postgresql

COPY ./extension /extension
RUN cd /extension && ./make.sh
