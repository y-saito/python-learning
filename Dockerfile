FROM python:3.12-slim

WORKDIR /workspace

# Keep Python output unbuffered for easier logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY app ./app

CMD ["python", "app/main.py"]
