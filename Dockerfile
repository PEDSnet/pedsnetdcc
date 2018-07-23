FROM ubuntu:bionic

RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -my wget gnupg

RUN apt-get install -y tzdata

RUN apt-get install wget ca-certificates

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main 9.5" >> /etc/apt/sources.list.d/postgresql.list'

# use postgresql version that matches the server for pg_dump/pg_restore
RUN apt-get update && apt-get install -y apt-utils git-all libpq-dev graphviz graphviz-dev pkg-config postgresql-9.5

#RUN wget http://www.cpan.org/src/5.0/perl-5.28.0.tar.gz
#RUN tar xvfz perl-5.28.0.tar.gz
#RUN cd perl-5.28.0 && ./Configure -Duseithreads -des && make && make test && make install
#/usr/local/bin/cpan -u

# Install perl modules 
RUN apt-get install -y cpanminus

# Build using the current version of BMI on GitHub
RUN cpanm git://github.com/PEDSnet/PEDSnet-Derivation
RUN cpanm git://github.com/PEDSnet/PEDSnet-Derivation-BMI
RUN cpanm git://github.com/PEDSnet/PEDSnet-Derivation-Anthro_Z

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

RUN cpanm DBD::Pg

COPY . /app/

RUN set -xe \
    && apt-get update \
    && apt-get install -y python-pip
RUN pip install --upgrade pip
RUN cd /app/ && pip install -r requirements.txt && python setup.py install

RUN useradd -m -s /bin/bash normalized
#USER normalized
WORKDIR /app

ENTRYPOINT ["pedsnetdcc"]
CMD ["--help"]
