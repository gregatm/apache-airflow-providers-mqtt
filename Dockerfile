from "docker.io/apache/airflow:latest"

RUN pip install \
                 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3-2/constraints-3.14.txt  \
                 apache-airflow-providers-mqtt \
                 apache-airflow-providers-redis
