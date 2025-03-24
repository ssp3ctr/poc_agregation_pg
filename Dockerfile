# Используем Python 3.11
FROM python:3.11

# Устанавливаем зависимости
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

# Запускаем setup_db.py перед FastAPI
CMD ["/bin/bash", "-c", "uvicorn main:app --host 0.0.0.0 --port 8000"]
# CMD ["/bin/bash", "-c", "uvicorn main:app --host 0.0.0.0 --port 8000 && python setup_db.py"]