FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_green_data.py ingest_green_data.py

# ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "ingest_green_data.py" ]