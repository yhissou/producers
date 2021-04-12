# Producer HR Data

The goal of our producer is to get data from Rest API : GoRest and send data in Topic Pulsar Hoster in Kesque Platform.
If you want to use it you have to register in Kesque and gorest then change only configuration file - consumer.conf et crt.

## The Architecutre

```
├───configuration : config files ()
├───db : the sqlite db file is used to store id to get from Rest API, the best way should be to use filter in Rest API 
├───ddl : SQL script(s) for creating the database tables
├───logs : the logs by python scrpits
├───src : python scripts
└───tests : test scripts
└───Dockerfile : My Docker File to create a environment and run my producer
└───requirements.txt : Librairies used in this project
```

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Run the Producer

To run this project you can use Dockerfile or use Venv in your local environment
 
 Using Docker : 
    - Create your image based on my docker File : ``` docker build -t producers . ``` then launch ``` ```
 
 Using Venv : 
    - Create a virtual environment using pipenv and then install librairies using ```pip3 install -r requirements.txt```
    - Run hr_producer located in src/producers/hr_producer
    

### For the unit test

Run the code below to test the hr producer

```
pytest -q test.py
```

## Deployment


## Authors

* **Youssef HISSOU**