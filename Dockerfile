FROM apache/airflow:2.7.1

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        python3-pip \
        python3-dev \
        build-essential

# Install dbt
RUN pip install \
    dbt-core \
    dbt-postgres

USER airflow