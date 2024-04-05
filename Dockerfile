FROM python:3.11

RUN mkdir /build
RUN mkdir /config
RUN mkdir /download

COPY . /build
WORKDIR /build
RUN pip install --upgrade pip
RUN pip install .

COPY ./docker-entrypoint.sh /
RUN chmod 775 /docker-entrypoint.sh

WORKDIR /usr/src/app

ENTRYPOINT ["/docker-entrypoint.sh"]