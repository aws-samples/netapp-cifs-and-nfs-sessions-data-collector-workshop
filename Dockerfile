FROM amazonlinux:2023@sha256:d8323b3ea56d286d65f9a7469359bb29519c636d7d009671ac00b5c12ddbacf0
WORKDIR /usr/app
COPY requirements.txt /usr/app/
RUN yum install -y git python3-pip && yum clean all && pip install -r requirements.txt --no-cache-dir
HEALTHCHECK CMD [ "curl -f http://localhost/ || exit 1" ]