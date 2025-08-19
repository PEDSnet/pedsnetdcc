FROM ubuntu:jammy

RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -my wget gnupg

RUN apt-get install -y tzdata

RUN apt-get install wget ca-certificates

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

#RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main 9.5" >> /etc/apt/sources.list.d/postgresql.list'

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt jammy-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'

#RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt focal-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'

#RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt bionic-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'

ENV DEBIAN_FRONTEND noninteractive

ARG APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1

# use postgresql version that matches the server for pg_dump/pg_restore
RUN apt-get update && apt-get install -y apt-utils git-all libpq-dev graphviz graphviz-dev pkg-config  postgresql-15

RUN apt-get update && \
    apt-get install -y \
    libgit2-dev \
    zlib1g-dev \
    cargo \
    libxml2-dev \
    libssl-dev \
    libfontconfig1-dev \
    libmariadbclient-dev \
    libmariadb-client-lgpl-dev-compat \
    #libmariadb-client-lgpl-dev \
    software-properties-common \
    curl

#RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9

#RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'

#RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu  bionic-cran35/'

#RUN apt-get update && apt-get install -y --no-install-recommends build-essential r-base
# From https://cran.r-project.org/bin/linux/ubuntu/
# install two helper packages we need
RUN apt install -y --no-install-recommends software-properties-common dirmngr
# import the signing key (by Michael Rutter) for these repo
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
# updated to 4.4 see https://cran.r-project.org/bin/linux/ubuntu/
# add the R 4 repo from CRAN -- adjust 'focal' to 'groovy' or 'bionic' as needed
ARG R_VER=4.4
RUN add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
#  install R and its dependencies
RUN apt install -y --no-install-recommends build-essential r-base
# add the current R 4.0 or later ‘c2d4u’ repository
RUN add-apt-repository ppa:c2d4u.team/c2d4u4.0+
#add the key id for this repository, add the repository, and update the index
RUN apt install -y r-cran-tidyverse

#RUN apt install -y r-cran-int64

# Copy install packages script
COPY install_packages.sh /usr/local/bin

# Give execute permissions to install packages script
RUN chmod +x /usr/local/bin/install_packages.sh

# Run install packages scripts (including devtools)
RUN ./usr/local/bin/install_packages.sh

# Copy Argos install directory to container
ADD ohdsi-argos-master /usr/local/bin/ohdsi-argos-master
ADD int64 /usr/local/bin/int64

# Install it with devtools
RUN R -e 'setRepositories(ind=1:6); \
  options(repos="http://cran.rstudio.com/"); \
  if(!require(devtools)) { install.packages("devtools") }; \
  library(devtools); \
  install("/usr/local/bin/ohdsi-argos-master"); \
  install("/usr/local/bin/int64"); \
  install("/usr/local/bin/ohdsi-argos-master"); \
#  install_version("tidyverse", version = "1.3.2", repos = "http://cran.us.r-project.org");'

   install_version("dbplyr", version = "2.3.4", repos = "http://cran.us.r-project.org");'


#  install_version("DBI", version = "0.7", repos = "http://cran.us.r-project.org"); \
#  install_version("dplyr", version = "0.8.3", repos = "http://cran.us.r-project.org"); \
#  install_version("dbplyr", version = "1.4.2", repos = "http://cran.us.r-project.org"); \
#  install_version("lubridate", version = "1.7.4", repos = "http://cran.us.r-project.org"); \
#  install_version("purrr", version = "0.3.3", repos = "http://cran.us.r-project.org"); \
#  install_version("readr", version = "1.3.1", repos = "http://cran.us.r-project.org"); \
#  install_version("rlang", version = "0.4.1", repos = "http://cran.us.r-project.org"); \
#  install_version("stringr", version = "1.4.0", repos = "http://cran.us.r-project.org"); \
#  install_version("tibble", version = "2.1.3", repos = "http://cran.us.r-project.org"); \
#  install_version("tidyr", version = "1.0.0", repos = "http://cran.us.r-project.org"); \
#  install_version("ggplot2", version = "3.2.1", repos = "http://cran.us.r-project.org");'

#RUN wget http://www.cpan.org/src/5.0/perl-5.28.0.tar.gz
#RUN tar xvfz perl-5.28.0.tar.gz
#RUN cd perl-5.28.0 && ./Configure -Duseithreads -des && make && make test && make install
#/usr/local/bin/cpan -u

# Install perl modules 
RUN apt-get install -y perl cpanminus

# Build using the current version of BMI on GitHub
RUN cpanm MooX::Role::Chatty -f 
RUN cpanm https://github.com/PEDSnet/PEDSnet-Derivation.git
RUN cpanm https://github.com/PEDSnet/PEDSnet-Derivation-BMI.git
RUN cpanm https://github.com/PEDSnet/PEDSnet-Derivation-Anthro_Z.git

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    libjpeg-dev \
    libfreetype6 \
    libfreetype6-dev \
    libdbi-perl\
    libdbd-pg-perl  \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN cpanm Test::Simple
RUN cpanm Bundle::DBI
#RUN cpanm DBD::Pg

RUN add-apt-repository universe

ADD get-pip.py get-pip.py

RUN set -xe \
    && apt-get update \
    && apt-get install -y python2
#    && apt-get install -y python-pip
#    && apt-get install -y python-dev
#ADD https://bootstrap.pypa.io/get-pip.py get-pip.py
#RUN python get-pip.py
#RUN  apt-get install -y python-pip
#RUN pip install --upgrade pip
#RUN python2 -m pip install pip==19.0

RUN python2 get-pip.py

RUN apt-get install -y python-is-python2

RUN apt-get install -y python-dev

#RUN mkdir /usr/local/share/ca-certificates/extra

#COPY netscope-int.pem /usr/local/share/ca-certificates/extra/netscope-int.crt

#COPY netscope-root.pem /usr/local/share/ca-certificates/extra/netscope-root.crt

#RUN dpkg-reconfigure ca-certificates

COPY . /app/

RUN pip2 install --no-index /app/pip_requirements/click-6.7

RUN pip2 install --no-index /app/pip_requirements/docopt-0.6.2

RUN pip2 install --no-index /app/pip_requirements/six-1.17.0

RUN pip2 install --no-index /app/pip_requirements/scandir-1.10.0

RUN pip2 install --no-index /app/pip_requirements/typing-3.10.0.0

RUN pip2 install --no-index /app/pip_requirements/contextlib2-0.6.0.post1

RUN pip2 install --no-index /app/pip_requirements/pathlib2-2.3.7.post1

RUN pip2 install --no-index /app/pip_requirements/configparser-3.5.0

RUN pip2 install --no-index --no-build-isolation --find-links /app/pip_requirements /app/pip_requirements/setuptools_scm-5.0.2

RUN pip2 install --no-index --no-build-isolation --find-links /app/pip_requirements /app/pip_requirements/zipp-0.5.1

RUN pip2 install --no-index --no-build-isolation --find-links /app/pip_requirements /app/pip_requirements/importlib_metadata-2.1.3

# RUN pip2 install --no-index /app/pip_requirements/sqlalchemy-1.4.54

RUN pip2 install --no-index --no-build-isolation --find-links /app/pip_requirements /app/pip_requirements/SQLAlchemy-1.3.0

RUN pip2 install --no-index /app/pip_requirements/pygraphviz-1.3.1

RUN pip2 install --no-index /app/pip_requirements/ERAlchemy-0.0.28

RUN pip2 install --no-index /app/pip_requirements/itsdangerous-0.24

RUN pip2 install --no-index /app/pip_requirements/MarkupSafe-0.23

RUN pip2 install --no-index /app/pip_requirements/Jinja2-2.9.4

RUN pip2 install --no-index /app/pip_requirements/logutils-0.3.3

# RUN pip2 install /app/pip_requirements/MarkupSafe-0.23

RUN pip2 install --no-index --no-build-isolation --find-links /app/pip_requirements /app/pip_requirements/psycopg2-binary-2.7.7

# RUN pip2 install /app/pip_requirements/pygraphviz-1.3.1

RUN pip2 install --no-index /app/pip_requirements/sh-1.12.9

RUN pip2 install --no-index /app/pip_requirements/Werkzeug-0.11.15

# RUN pip2 install /app/pip_requirements/wheel-0.24.0

# RUN pip2 install /app/pip_requirements/SQLAlchemy-1.3.0

RUN pip2 install --no-index /app/pip_requirements/Flask-0.12.3

RUN pip2 install --no-index /app/pip_requirements/github-webhook-1.0.2

RUN pip2 install --no-index --no-build-isolation --find-links /app/pip_requirements /app/pip_requirements/pytest-runner-5.2

RUN pip2 install --no-index /app/pip_requirements/chardet-3.0.2

RUN pip2 install --no-index /app/pip_requirements/idna-2.5

RUN pip2 install --no-index /app/pip_requirements/urllib3-1.24

RUN pip2 install --no-index /app/pip_requirements/certifi-2017.4.17

RUN pip2 install --no-index --no-build-isolation --find-links /app/pip_requirements /app/pip_requirements/requests-2.20.0

RUN pip2 install --no-index /app/pip_requirements/simpleflock-0.0.3

#RUN set -xe \
#    && apt-get update \
#    && apt-get install -y python-pip
#RUN pip install --upgrade pip
# RUN cd /app/ && pip2 install -r requirements.txt && python setup.py install

RUN cd /app/ && python setup.py install

ENV PYTHONPATH="/app/data-models-sqlalchemy:/app/data-models-sqlalchemy/dmsa"

#USER normalized
RUN useradd -m -s /bin/bash normalized

RUN mkdir /output

RUN chmod 755 /output

WORKDIR /app

ENTRYPOINT ["pedsnetdcc"]
CMD ["--help"]
