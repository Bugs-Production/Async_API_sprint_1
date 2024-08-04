FROM python:3.11

WORKDIR /fastapi_movies

COPY requirements.txt /fastapi_movies/requirements.txt

RUN pip install --no-cache-dir -r /fastapi_movies/requirements.txt

COPY src /fastapi_movies/src
COPY cronfile /etc/cron.d/postgres_to_elastic
COPY .env /fastapi_movies/

# создание директории и файла для логов крона
RUN apt-get update && apt-get install -y cron \
    && mkdir -p /var/log/cron \
    && touch /var/log/cron/cron.log \
    && chmod 664 /var/log/cron/cron.log

RUN chmod 0644 /etc/cron.d/postgres_to_elastic \
    && crontab /etc/cron.d/postgres_to_elastic

CMD ["cron", "-f"]
