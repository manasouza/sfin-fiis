# FROM python:3.10.12-alpine3.18
FROM python:3.10.12-slim-bullseye

RUN pip -V

ADD ./* /opt/sfin-fiis
WORKDIR /opt/sfin-fiis

RUN ls -lh
RUN pip install -r requirements.txt

ENTRYPOINT [ "python main_local.py" ]
