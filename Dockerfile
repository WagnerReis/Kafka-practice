FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Update package list and install dependencies
RUN apt-get update && \
    apt-get install -y build-essential librdkafka-dev

# Install Python dependencies (if you have a requirements.txt file)
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Keep the container running
CMD ["tail", "-f", "/dev/null"]
