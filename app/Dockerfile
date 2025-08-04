FROM python:3.9-slim
RUN groupadd -r deciphercollector && useradd --no-create-home -g deciphercollector -r deciphercollector
USER root
WORKDIR /usr/netappcollector/
COPY requirements.txt /requirements.txt
RUN apt-get update && apt-get install -y build-essential curl software-properties-common git && rm -rf /var/lib/apt/lists/* && pip3 install --upgrade pip && pip3 install -r /requirements.txt
EXPOSE 8080
HEALTHCHECK CMD curl --fail http://localhost:8080/_stcore/health