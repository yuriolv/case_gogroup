FROM apache/airflow:3.1.8

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH=/opt/airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"