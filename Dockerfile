FROM python:3.11-slim

# Install Java and 'procps' (fixes that 'ps: command not found' warning)
RUN apt-get update && apt-get install -y default-jre procps && apt-get clean

WORKDIR /app

# Copy requirements and install (Docker will CACHE this now)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

CMD ["streamlit", "run", "utils/dashboard.py", "--server.port", "8501", "--server.address", "0.0.0.0"]