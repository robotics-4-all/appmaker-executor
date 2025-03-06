FROM python:3.10

WORKDIR /appexecutor

RUN apt-get update && apt upgrade  -y

RUN pip install python-dotenv

RUN pip install https://github.com/robotics-4-all/commlib-py/archive/devel.zip -U

COPY ./appmakerexecutor /appexecutor

COPY ./entrypoint.sh /entrypoint.sh

ENV USE_REDIS=True
ENV BROKER_TYPE=MQTT
ENV BROKER_HOST=localhost
ENV BROKER_PORT=1883
ENV BROKER_SSL=True
ENV BROKER_USERNAME=guest
ENV BROKER_PASSWORD=guest
ENV UID=123

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]

