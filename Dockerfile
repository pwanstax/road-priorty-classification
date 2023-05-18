# Base image
FROM openjdk:8-jdk

# Set the working directory
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    python3 \
    python3-pip

# Set the Python 3 as the default Python version
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Install PySpark
RUN pip3 install pyspark==3.2.0

# Set environment variables
ENV PYSPARK_PYTHON=python3

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy your application code
COPY . .

# Expose the necessary ports
EXPOSE 8000

# Set the entrypoint command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
