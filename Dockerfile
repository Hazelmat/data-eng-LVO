FROM ghcr.io/flyteorg/flytekit:py3.11-1.9.1

WORKDIR /ROOT
USER root
RUN apt-get -y install libgeos-dev

ENV PYTHONPATH /ROOT

COPY requirements.txt ./
COPY requirements-dev.txt ./
COPY entrypoint.sh entrypoint.sh

RUN pip install -r ./requirements.txt


COPY src $PYTHONPATH/src

ARG tag


ENTRYPOINT ["bash", "entrypoint.sh"]





