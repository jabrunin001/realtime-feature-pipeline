FROM python:3.11-slim
RUN pip install mlflow==2.13.0 boto3==1.34.0 && mkdir /mlflow
