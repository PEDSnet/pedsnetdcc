FROM python:2

RUN apt-get update && apt-get install -y apt-utils libpq-dev graphviz graphviz-dev pkg-config

COPY . /app/

RUN cd /app/ && pip install -r requirements.txt && python setup.py install

ENTRYPOINT ["pedsnetdcc"]
CMD ["--help"]
