FROM python:3.10

WORKDIR /appexecutor

COPY requirements.txt /appexecutor/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./appmakerexecutor /appexecutor

COPY ./entrypoint.sh /entrypoint.sh

ENV USE_REDIS=True \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]

