FROM apache/airflow:2.10.5

USER root
COPY requirements.txt /requirements.txt

RUN apt-get update && apt-get install -y git bash curl && apt-get clean

RUN apt-get update && \
    apt-get install -y curl unzip && \
    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && \
    mv mc /usr/local/bin/mc

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# docker build -t my-airflow-image2:2.10.5 .