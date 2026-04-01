FROM python:3.12-slim

# вisable buffering and creation of .pyc files
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# сopy source code
COPY ./src ./src

CMD ["python", "-m", "src.main"]