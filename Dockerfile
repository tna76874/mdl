FROM python:3.7.7



RUN mkdir /build
RUN mkdir /config
RUN mkdir /download

COPY . /build
WORKDIR /build
RUN pip install --upgrade pip
RUN pip install .

WORKDIR /usr/src/app

CMD [ "/usr/local/bin/mdl" , "--config", "/config", "--download", "/download", "--run"]
