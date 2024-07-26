FROM python:3.12.4

WORKDIR /etl
COPY . .
RUN python3 -m venv .venv
RUN chmod -R 777 .venv
RUN .venv/bin/pip install -r requrements.txt

# RUN python3 -m venv .venv
# ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY