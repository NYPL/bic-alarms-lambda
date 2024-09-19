FROM python:3.12

RUN apt-get update && \
	apt-get install -y chromium
RUN apt-get install dumb-init

ADD . /src
WORKDIR /src

COPY requirements.txt ./
RUN pip install --upgrade pip && \
	pip install -r requirements.txt

COPY . .
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD [ "python3", "./main.py"]
