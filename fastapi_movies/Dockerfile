FROM python:3.11

WORKDIR fastapi_movies/src

COPY requirements.txt /fastapi_movies/requirements.txt

RUN pip install --no-cache-dir -r /fastapi_movies/requirements.txt

COPY src /fastapi_movies/src

CMD ["fastapi", "run", "main.py", "--port", "80"]
