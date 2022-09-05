FROM python:3.8

RUN mkdir -p /home/celery_app
WORKDIR /home/celery_app

COPY requirements.txt /home/celery_app

RUN pip install -r /home/celery_app/requirements.txt

COPY . /home/celery_app

