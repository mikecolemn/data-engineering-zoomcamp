FROM python:3.9

RUN pip install pandas

WORKDIR /app
copy pipeline.py pipeline.py

# ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "pipeline.py" ]
