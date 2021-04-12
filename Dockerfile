FROM python:3.8-slim-buster

WORKDIR /producers

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENV PYTHONPATH /producers/

CMD [ "python", "-u",  "./src/producers/hr_producer.py" ]
