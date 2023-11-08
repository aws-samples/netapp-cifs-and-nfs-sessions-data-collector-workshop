FROM python
WORKDIR /usr/app
COPY requirements.txt /usr/app/
RUN pip install -r requirements.txt