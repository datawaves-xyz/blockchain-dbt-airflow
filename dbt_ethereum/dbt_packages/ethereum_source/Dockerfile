FROM python:3.9.9-slim-bullseye

ARG dbt_core_ref=dbt-core@v1.1.0b1
ARG dbt_spark_ref=dbt-spark@v1.0.0
# special case args
ARG dbt_spark_version=all
ARG dbt_third_party

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
  python-dev \
  libsasl2-dev \
  libsasl2-modules  \
  gcc \
  unixodbc-dev \
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

# Install dbt spark
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_spark_ref}#egg=dbt-spark[${dbt_spark_version}]"

# Copy files to workspace
RUN mkdir /root/.dbt
COPY profiles.yml /root/.dbt
RUN mkdir /root/ethereum
WORKDIR /ethereum
COPY . .

RUN dbt deps
CMD ["dbt", "run"]