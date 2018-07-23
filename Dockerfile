FROM python:2

RUN apt-get install wget ca-certificates

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main 9.5" >> /etc/apt/sources.list.d/postgresql.list'

# use postgresql version that matches the server for pg_dump/pg_restore
RUN apt-get update && apt-get install -y apt-utils libpq-dev graphviz graphviz-dev pkg-config postgresql-9.5

COPY . /app/

RUN cd /app/ && pip install -r requirements.txt && python setup.py install

ENTRYPOINT ["pedsnetdcc"]
CMD ["--help"]
