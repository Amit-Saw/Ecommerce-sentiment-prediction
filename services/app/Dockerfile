FROM python:3.9-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY app.py /app/app.py
COPY templates /app/templates
ENV PYTHONUNBUFFERED=1
EXPOSE 5000
ENTRYPOINT ["python", "/app/app.py"]
