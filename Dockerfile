FROM python:3.9-slim

ARG pip_source=https://mirror.sjtu.edu.cn/pypi/web/simple/

WORKDIR /app

COPY requirements.txt .

RUN pip install -U pip -i ${pip_source} && \
    pip install -r requirements.txt -i ${pip_source} && \
    rm requirements.txt

COPY pypsmb ./pypsmb/
COPY *.py .
COPY config.yml .

EXPOSE 13880

ENTRYPOINT python3 main.py