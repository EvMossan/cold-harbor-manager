# docker/airflow/Dockerfile

FROM apache/airflow:3.1.4

# Copy requirements from the build context (project root)
COPY requirements_airflow.txt /requirements_airflow.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements_airflow.txt