import json
import requests
from ratelimit import limits, sleep_and_retry
import datetime
import time
import copy
import logging
import src.utils.tools as T
import pulsar
import time
from datetime import datetime

# Load configuration
config = T.get_configuration("/producers/conf/producer.conf")

# API Configuration
sixty_seconds = int(config["API"]["sixty_seconds"])
nb_calls = int(config["API"]["nb_calls"])
api_url_base = config["API"]["api_url_base"]
api_token = config["API"]["api_token"]

# PULSAR Configuration
service_url = config.get('PULSAR','service_url')
pulsar_token = config["PULSAR"]["token"]
topic_name = config["TOPIC.HR"]["topic_users_data"]
trust_certs = config["PULSAR"]["certs"]

# File & Directory configuration
app_name = 'hr_users_producer'

# SQLite Config
db_file_path = config["SQLITE"]["db_file_path"]

# Two functons decorators that can be used to
# Return a wrapped function that rescues rate limit exceptions, sleeping the
# current thread until rate limit resets.
@sleep_and_retry
@limits(calls=nb_calls, period=sixty_seconds)
def call_api(api_url_base, api_token, id):
    """
    This method is used to call an Api and return response as Json
    :param api_url_base: Url used to call the API
    :param match_id: Id match
    :return: returns api response as json format
    """
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0',
        'Retry-After': '60',
        'Authorization': 'Bearer {0}'.format(api_token)
    }
    url = '{0}/{1}'.format(api_url_base, str(id))
    logging.info('Call API : %s', url)

    response = requests.get(url, headers=headers)#, params=payload)
    if response.status_code in [429, 502]:
        error_to_diplay = 'Too many requests,' if response.status_code == 429 else 'Bad Gateway,'
        logging.info('%s waiting 30 seconds to retry' % error_to_diplay )
        time.sleep(30)
        response = requests.get(url, headers=headers)

    elif response.status_code != 200:
        logging.error('API response: %s', response)
        raise Exception('API response: {}'.format(response.status_code))
    return json.loads(response.content.decode('utf-8'))

def send_callback(res, msg_id):
    logging.info('Message published res=%s', res)

if __name__ == "__main__":

    start_time = time.time()

    T.init_logger('a', app_name)

    logging.info("Start Producer at : {}".format(datetime.now()))

    try:
        # Init the client
        client = T.init_client(service_url, trust_certs, pulsar_token)

        # Init the producer without schema to send max of data, I'll control data in consumer
        # I enable batching, the producer will accumulate and send a batch of messages in a single request.
        producer = client.create_producer(
            topic_name,
            batching_enabled=True,
            batching_max_messages=10,
            batching_max_allowed_size_in_bytes=128 * 1024,
            batching_max_publish_delay_ms=100
        )

        # Call the API with SQLITE
        list_ids = T.get_users_id_from_db(db_file_path)
        logging.info(" ID to get from Rest API : {}".format(list_ids))
        for id in list_ids:
            msg = call_api(api_url_base, api_token, id)
            if msg.get("code") != 404: # User not found, we don't have to send if in the topic
                # Send the message, I use sync method because I want to be sure that the message is delivered and available in the topic.
                logging.info('send msg {}'.format(msg))
                producer.send(json.dumps(msg).encode('utf-8'),partition_key=str(id)) #Partition Key can be useful for topic compactioned
            else:
                logging.warning(" Message not found for id {}".format(id))

        logging.info(" Time Duration {} seconds for {} messages".format(str(time.time() - start_time),len(list_ids)))

    except Exception as e:
        logging.error("there is a problem in consumer : {0}".format(str(e)))
        logging.info(" Time Duration for {} messages".format(str(time.time() - start_time),len([])))

        # If there is an issue I stop the producer
        raise
