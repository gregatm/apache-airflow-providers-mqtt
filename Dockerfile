from "docker.io/apache/airflow:slim-latest"

RUN pip install \
                 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.14.txt  \
                 psycopg2-binary \
                 asyncpg \
                 apache-airflow-providers-http  \
                 apache-airflow-providers-redis \
                 apache-airflow-providers-fab[oauth] \
                 apache-airflow-providers-ftp \
                 apache-airflow-providers-sftp \
                 apache-airflow-providers-celery \
                 apache-airflow-providers-common-sql \
                 apache-airflow-providers-imap \
                 apache-airflow-providers-smtp \
                 apache-airflow-providers-mqtt
