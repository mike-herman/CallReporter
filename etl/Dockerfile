FROM python:3.12.4

WORKDIR /etl
COPY . .
RUN python3 -m venv .venv && chmod -R 777 .venv
# RUN chmod -R 777 .venv
RUN .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
# ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
CMD /usr/bin/bash
