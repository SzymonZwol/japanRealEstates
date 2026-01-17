FROM astrocrpublic.azurecr.io/runtime:3.1-10

# ===============================
# Install MS SQL ODBC Driver 18
# ===============================
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
 && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
 && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Back to airflow user
USER airflow
