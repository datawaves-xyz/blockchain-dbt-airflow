# Top level build args
ARG build_for=linux/amd64
ARG airflow_version=2.2.2
ARG python_version=3.9

##
# base image (abstract)
##
FROM --platform=$build_for apache/airflow:${airflow_version}-python${python_version} as base

ARG dbt_core_ref=dbt-core@v1.2.0a1
ARG dbt_postgres_ref=dbt-core@v1.2.0a1
ARG dbt_redshift_ref=dbt-redshift@v1.0.0
ARG dbt_bigquery_ref=dbt-bigquery@v1.0.0
ARG dbt_snowflake_ref=dbt-snowflake@v1.0.0
ARG dbt_spark_ref=dbt-spark@1.1.0rc1
# special case args
ARG dbt_spark_version=all
ARG dbt_third_party

USER root

ENV ACCEPT_EULA=Y

# System setup
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

USER airflow

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

##
# dbt-postgres
##
FROM base as dbt-airflow-postgres
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_postgres_ref}#egg=dbt-postgres&subdirectory=plugins/postgres"


##
# dbt-redshift
##
FROM base as dbt-airflow-redshift
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_redshift_ref}#egg=dbt-redshift"


##
# dbt-bigquery
##
FROM base as dbt-airflow-bigquery
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_bigquery_ref}#egg=dbt-bigquery"


##
# dbt-snowflake
##
FROM base as dbt-airflow-snowflake
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_snowflake_ref}#egg=dbt-snowflake"


##
# dbt-airflow-spark
##
FROM base as dbt-airflow-spark

USER root
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    python-dev \
    libsasl2-dev \
    gcc \
    unixodbc-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

USER airflow
RUN python -m pip install --no-cache-dir "git+https://github.com/datawaves-xyz/dbt-spark.git@jerco/wip-faster-caching-option2#egg=dbt-spark[PyHive]"
