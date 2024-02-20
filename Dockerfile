FROM amazonlinux:2023
WORKDIR /usr/app
COPY requirements.txt /usr/app/
RUN yum install -y git python3-pip && pip install -r requirements.txt