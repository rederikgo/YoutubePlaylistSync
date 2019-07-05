FROM python:3.7.3-slim-stretch

RUN python -m pip install --upgrade pip && pip install --upgrade setuptools

RUN mkdir /YoutubePlaylistSync
WORKDIR /YoutubePlaylistSync

COPY ./requirements.txt /YoutubePlaylistSync/requirements.txt

RUN pip install -r requirements.txt

CMD ["python", "./yps.py"]