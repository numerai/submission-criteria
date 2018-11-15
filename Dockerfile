FROM python:3.6

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -qq update
RUN apt-get -qq install -y --no-install-recommends apt-utils
RUN apt-get -qq install mysql-server
RUN apt-get -qq install default-libmysqlclient-dev
RUN apt-get -qq install gcc

WORKDIR /app
COPY ./setup.py ./
COPY ./submission_criteria ./submission_criteria

RUN pip install -e .

ARG env=prod
ARG secrets_bucket=numerai-api-ml-secrets
ENV S3_SECRETS_BUCKET=$secrets_bucket
ENV PORT=4000 REPLACE_OS_VARS=true SHELL=/bin/bash
ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

EXPOSE 4000

CMD ["./submission_criteria/server.py"]
