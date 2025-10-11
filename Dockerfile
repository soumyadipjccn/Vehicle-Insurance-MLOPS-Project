FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 5000

# <-- Fix here: point to app.py
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]


