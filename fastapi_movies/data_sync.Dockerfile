FROM python:3.11

WORKDIR /fastapi_movies

COPY requirements.txt /fastapi_movies/requirements.txt

RUN pip install --no-cache-dir -r /fastapi_movies/requirements.txt

COPY src /fastapi_movies/src
COPY cronfile /etc/cron.d/postgres_to_elastic
COPY .env /fastapi_movies/

RUN chmod 0644 /etc/cron.d/postgres_to_elastic

RUN apt-get update && apt-get install -y cron
RUN crontab /etc/cron.d/postgres_to_elastic

CMD ["cron", "-f"]
