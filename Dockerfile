FROM apache/airflow:2.6.3

COPY requirement.txt /

RUN pip freeze > requirements.txt
USER root