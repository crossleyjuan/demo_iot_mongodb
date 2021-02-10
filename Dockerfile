FROM ubuntu:18.04

ENV TZ=Europe/Madrid
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone; \
    apt upgrade; \
    apt update; \
    apt install python3-dev python3-pip python3-setuptools wget alien cmake build-essential alien libcups2-dev -y

WORKDIR /app
RUN mkdir -p /app

COPY . /app

RUN pip3 install -r requirements.txt; \
    chmod +x run.sh; \
    chmod +x entry-point.sh;

ENTRYPOINT ["/app/entry-point.sh"]
CMD ["/app/run.sh"]
