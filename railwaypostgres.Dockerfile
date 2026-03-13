FROM ghcr.io/dbsystel/postgresql-partman@sha256:ba56c1e48c2d92c8b58064c0d70d70808dd683ee40c0c2649c6ef0209d0c022e

ARG PGCRON_VERSION="1.6.2"

USER root

RUN apt-get update && apt-get install -y wget build-essential
RUN cd /tmp \
    && wget "https://github.com/citusdata/pg_cron/archive/refs/tags/v${PGCRON_VERSION}.tar.gz" \
    && tar zxf "v${PGCRON_VERSION}.tar.gz" \
    && cd "pg_cron-${PGCRON_VERSION}" \
    && make \
    && make install \
    && cd /tmp \
    && rm -rf "pg_cron-${PGCRON_VERSION}" "v${PGCRON_VERSION}.tar.gz"

RUN echo "cron.database_name = 'tycho_indexer_0'" >> /opt/bitnami/postgresql/conf/postgresql.conf

# Do not switch back to USER 1001 on Railway
# USER 1001
