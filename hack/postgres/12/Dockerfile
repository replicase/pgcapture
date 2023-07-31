FROM postgres:12-alpine

RUN wget https://github.com/rueian/pglogical/archive/REL2_3_4_no_filter.tar.gz && \
    tar -zxvf REL2_3_4_no_filter.tar.gz && \
    apk -U add --no-cache build-base libxslt-dev libxml2-dev openssl-dev libedit-dev zlib-dev $DOCKER_PG_LLVM_DEPS krb5-pkinit krb5-dev krb5 && \
    cd /pglogical-REL2_3_4_no_filter && \
    make USE_PGXS=1 CPPFLAGS="-DPGL_NO_STDIN_ASSIGN" clean all && \
    make install && \
    cd / && \
    rm -rf /REL2_3_4_no_filter.tar.gz /pglogical-REL2_3_4_no_filter

RUN chown -R postgres:postgres /usr/local/share/postgresql/extension
RUN chown -R postgres:postgres /usr/local/lib/postgresql

COPY ./extension /extension
RUN cd /extension && ./make.sh
