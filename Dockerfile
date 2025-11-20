FROM python:3.12-slim

WORKDIR /app
COPY process.py /app/process.py
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "process.py"]
