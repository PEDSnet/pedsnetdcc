FROM python:2

RUN apt-get install libpq-dev

COPY . /app/

RUN cd /app/ && pip install -r requirements.txt && python setup.py install

ENTRYPOINT ["pedsnetdcc"]
CMD ["--help"]
