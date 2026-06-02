FROM python:3.11-slim
RUN apt-get update && apt-get install -y default-jre-headless curl libgomp1 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
WORKDIR /app
COPY pyproject.toml /app/
RUN pip install -e . 2>/dev/null || pip install \
    pyspark==3.5.1 delta-spark==3.2.0 kafka-python==2.0.2 redis==5.0.4 \
    lightgbm==4.3.0 mlflow==2.13.0 fastapi==0.111.0 uvicorn==0.30.1 \
    pandas==2.2.2 numpy==1.26.4 boto3==1.34.0
COPY . /app
