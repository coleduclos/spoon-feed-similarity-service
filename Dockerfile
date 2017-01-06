FROM python:latest

MAINTAINER cole.duclos

RUN apt-get update

RUN pip install boto3  

COPY *.py ./ 

ENTRYPOINT python run.py
