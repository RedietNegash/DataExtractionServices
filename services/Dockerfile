FROM python:3.8-slim

# Set the working directory inside the container
WORKDIR /usr/src/app/

# Ensure output is flushed immediately
ENV PYTHONUNBUFFERED 1

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Copy requirements file and install dependencies
COPY requirements.txt .  
RUN pip install --no-cache-dir -r requirements.txt

COPY . .



