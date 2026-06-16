# Dedicated Airflow image for the financial-data-lake project.
# Extends the official image with the project's runtime deps so they are
# baked in once (fast container start) rather than reinstalled on every
# boot via _PIP_ADDITIONAL_REQUIREMENTS.
FROM apache/airflow:3.1.8

COPY requirements.txt /requirements.txt

# pip runs as the unprivileged `airflow` user in this base image.
RUN pip install --no-cache-dir -r /requirements.txt
