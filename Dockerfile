FROM docker-io-remote.artifactory.devops.dnb.net:5000/python:3.12-slim-bullseye

WORKDIR /app

COPY requirements.txt /app/requirements.txt

COPY src/. /app

RUN pip install --upgrade pip && pip install -r requirements.txt

CMD ["python", "main.py"]
