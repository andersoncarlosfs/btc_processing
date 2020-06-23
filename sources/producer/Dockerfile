FROM python:latest

WORKDIR /usr/src/app

COPY ./requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY ./source ./source

EXPOSE 5000

CMD [ "python", "./source/producer.py"]