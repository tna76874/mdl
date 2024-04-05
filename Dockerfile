FROM python:3.11

RUN mkdir /build
RUN mkdir /config
RUN mkdir /download

COPY . /build
WORKDIR /build
RUN pip install --upgrade pip
RUN pip install . && pip install -r requirements.txt

RUN groupadd -r -g 1000 worker && \
    useradd -r -g worker -u 1033 -d /home/worker -s /bin/bash worker

COPY --chown=worker:worker ./docker-entrypoint.sh /
RUN chmod 775 /docker-entrypoint.sh

WORKDIR /home/worker

ENTRYPOINT ["/docker-entrypoint.sh"]