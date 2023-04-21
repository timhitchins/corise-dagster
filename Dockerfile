# ----------------------------------------- #
#                 Base
# ----------------------------------------- #
FROM rocker/geospatial:4.2.2 AS base

ENV COURSE_WEEK=week_2

ENV VIRTUAL_ENV=/opt/venv
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV POETRY_VERSION=1.1.12
ENV POETRY_HOME=/opt/poetry

# ----------------------------------------- #
#                 Builder
# ----------------------------------------- #
FROM base AS builder

# Install Python

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-dev python3-venv python3 python3-pip && \
    pip3 install --upgrade pip setuptools wheel

# Setup virtual env

RUN python3 -m venv "$VIRTUAL_ENV" && \
    mkdir -p "$DAGSTER_HOME"

ENV PATH="$VIRTUAL_ENV/bin:$POETRY_HOME/bin:$PATH"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# RUN apt-get update && \
#     apt-get -y upgrade && \
#     apt-get install -y --no-install-recommends \
#     curl \
#     build-essential \
#     libpq-dev && \
#     apt-get -y clean && \
#     rm -rf /var/lib/apt/lists/* && \
#     curl -sSL https://install.python-poetry.org | python -

RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install --yes --no-install-recommends \
    unixodbc unixodbc-dev odbc-postgresql libssl-dev \
    libpq-dev libudunits2-dev libaio1 libaio-dev alien \
    gnupg2 curl build-essential && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/* && \
    # Poetry installation script
    curl -sSL https://install.python-poetry.org | python3 -

COPY poetry.lock pyproject.toml /
RUN pip install --no-cache-dir --upgrade pip==21.3.1 setuptools==60.2.0 wheel==0.37.1 && \
    poetry config virtualenvs.path "$VIRTUAL_ENV" && \
    poetry install --no-root --no-interaction --no-ansi --no-dev

# ODBC driver installation
# SQL Server
# https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16&tabs=ubuntu18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline 
ENV ACCEPT_EULA=Y 
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update \
    && apt-get --yes --no-install-recommends install -y msodbcsql18
#  krb5-user libgssapi-krb5-2

# # Oracle Instant Client Installation
# ENV ORA_DIR=/opt/oracle
# RUN mkdir -p ${ORA_DIR}

# # Download RPMs - https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
# ENV ORA_FTP_ENDPOINT=https://download.oracle.com/otn_software/linux/instantclient/218000/

# RUN wget ${ORA_FTP_ENDPOINT}/oracle-instantclient-basic-21.8.0.0.0-1.x86_64.rpm -P ${ORA_DIR} 
# RUN wget ${ORA_FTP_ENDPOINT}/oracle-instantclient-odbc-21.8.0.0.0-1.x86_64.rpm -P ${ORA_DIR} 
# RUN wget ${ORA_FTP_ENDPOINT}/oracle-instantclient-devel-21.8.0.0.0-1.x86_64.rpm -P ${ORA_DIR}


# # Install instant client and ODBC driver
# RUN alien -i ${ORA_DIR}/oracle-instantclient-basic-21.8.0.0.0-1.x86_64.rpm
# RUN alien -i ${ORA_DIR}/oracle-instantclient-odbc-21.8.0.0.0-1.x86_64.rpm
# RUN alien -i ${ORA_DIR}/oracle-instantclient-devel-21.8.0.0.0-1.x86_64.rpm

# # Set configs
# RUN export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib/oracle/21/client64/lib" && \
#     sh -c 'echo /usr/lib/oracle/21/client64/lib/ > /etc/ld.so.conf.d/oracle.conf' && \
#     ldconfig

# # Update ODBC odbcinst.ini
# RUN echo '' | tee -a /etc/odbcinst.ini
# RUN echo '[Oracle]' | tee -a /etc/odbcinst.ini
# RUN echo 'Description=Oracle driver' | tee -a /etc/odbcinst.ini
# RUN echo 'Driver=/usr/lib/oracle/21/client64/lib/libsqora.so.21.1' | tee -a /etc/odbcinst.ini
# RUN echo 'UsageCount=1' | tee -a /etc/odbcinst.ini
# RUN echo 'FileUsage=1' | tee -a /etc/odbcinst.ini


# RENV setup - https://rstudio.ghub.io/renv/articles/docker.html
# ENV RENV_VERSION=0.17.3
# ENV RSTUDIO_HOME=/home/rstudio
# ENV PROJECT_HOME=$RSTUDIO_HOME/GEMS_pipeline
# RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
# RUN R -e "remotes::install_github('rstudio/renv@$RENV_VERSION')"

# ----------------------------------------- #
#                  Runner
# ----------------------------------------- #
# FROM base AS runner

ENV COURSE_WEEK=week_2

ENV PATH="$VIRTUAL_ENV/bin:$POETRY_HOME/bin:$PATH"

RUN groupadd -r dagster && useradd -m -r -g dagster dagster
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
COPY --from=builder --chown=dagster $DAGSTER_HOME $DAGSTER_HOME
COPY --from=builder /usr/local/lib/R/site-library /usr/local/lib/R/site-library
COPY --from=builder $POETRY_HOME $POETRY_HOME
# ODBC
COPY --from=builder $ORA_DIR $ORA_DIR
COPY --from=builder /etc/odbcinst.ini /etc/odbcinst.ini
COPY --from=builder /etc/odbc.ini /etc/odbc.ini
WORKDIR $DAGSTER_HOME

# ----------------------------------------- #
#                  Dagit
# ----------------------------------------- #
FROM runner AS dagit
# USER dagster:dagster
EXPOSE 3000
CMD ["dagit", "-h", "0.0.0.0", "--port", "3000", "-w", "workspace.yaml"]

# ----------------------------------------- #
#                  Daemon
# ----------------------------------------- #
FROM runner AS daemon
# USER dagster:dagster
CMD ["dagster-daemon", "run"]

# ----------------------------------------- #
#              Code Locations
# ----------------------------------------- #
FROM runner AS content
ENV DAGSTER_CURRENT_IMAGE=corise-dagster-answer-key_content
ENV COURSE_WEEK=week_2

COPY ${COURSE_WEEK}/workspaces/ ./workspaces
USER dagster:dagster
EXPOSE 4000
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "workspaces/content/deployment.py"]

FROM runner AS project
ENV DAGSTER_CURRENT_IMAGE=corise-dagster-answer-key_project
ENV COURSE_WEEK=week_2

COPY ${COURSE_WEEK}/workspaces/ ./workspaces
USER dagster:dagster
EXPOSE 4001
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4001", "-f", "workspaces/project/deployment.py"]

FROM runner AS challenge
ENV DAGSTER_CURRENT_IMAGE=corise-dagster-answer-key_challenge
ENV COURSE_WEEK=week_2

COPY ${COURSE_WEEK}/workspaces/ ./workspaces
USER dagster:dagster
EXPOSE 4002
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4002", "-f", "workspaces/challenge/deployment.py"]

# ----------------------------------------- #
#                  Dev
# ----------------------------------------- #
FROM runner as dev
ENV PATH="$VIRTUAL_ENV/bin:$POETRY_HOME/bin:$PATH"
# RUN usermod -aG sudo rstudio && usermod -aG sudo dagster
USER root:root
# USER dagster:dagster
# USER rstudio:rstudio
WORKDIR /project
COPY . /project
# RUN sudo chhmod -R 777 .
EXPOSE 8787