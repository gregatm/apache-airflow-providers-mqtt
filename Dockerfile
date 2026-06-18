from "docker.io/apache/airflow:slim-latest"

RUN pip install \
                 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.14.txt  \
                 psycopg2-binary \
                 asyncpg \
                 apache-airflow[otel,http,redis,ftp,sftp,common.sql,imap,smtp,statsd] \
                 apache-airflow-providers-fab[oauth] \
                 apache-airflow-providers-celery \
                 apache-airflow-providers-mqtt
