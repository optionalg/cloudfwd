#!/usr/bin/python

import os
import time
import logging
import requests

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except:
    pass

try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass

logging.basicConfig(
    format='%(asctime)-15s mod=%(module)s func=%(funcName)s line=%(lineno)d %(message)s',
    level=logging.INFO)


CONNECTOR_URI = 'http://{}:8083/connectors'.format(
    os.environ.get('KAFKA_CONNECTOR_IP', 'kafkaconnect1'))
TOPIC = os.environ.get('KAFKA_CONNECT_TOPICS', 'perf')
JVM_HEAP_SIZE = os.environ.get('JVM_HEAP_SIZE', '8G')
TOKEN_WITH_ACK = '00000000-0000-0000-0000-000000000001'
DEFAULT_ADMIN = 'admin'
DEFAULT_PASSWORD = 'Chang3d!'
SOURCE_TYPE = 'generic_singleline_notimestamp'


def create_token(uri, data):
    auth = requests.auth.HTTPBasicAuth(DEFAULT_ADMIN, DEFAULT_PASSWORD)
    logging.info('posting to %s', uri)
    while 1:
        try:
            resp = requests.post(uri, data=data, auth=auth, verify=False)
        except Exception:
            logging.exception('failed to post to %s', uri)
            time.sleep(2)
        else:
            if resp.ok:
                return

            if resp.status_code == 409:
                # already exists
                return

            logging.error('failed to post to %s, error=%s', uri, resp.text)
            time.sleep(2)


def create_token_with_ack(host_name):
    data = {
        'name': 'hec-token-ack',
        'token': TOKEN_WITH_ACK,
        'index': 'main',
        'indexes': 'main',
        'useACK': '1',
        'disabled': '0',
        'sourcetype': SOURCE_TYPE
    }

    uri = 'https://{}:8089/servicesNS/nobody/splunk_httpinput/data/inputs/http?output_mode=json'.format(host_name)
    create_token(uri, data)


def enable_ack_idle_cleanup(host_name):
    auth = requests.auth.HTTPBasicAuth(DEFAULT_ADMIN, DEFAULT_PASSWORD)
    data = {
        'ackIdleCleanup': 1
    }
    uri = 'https://{}:8089//services/data/inputs/http/http?output_mode=json'.format(host_name)
    logging.info('posting to %s', uri)
    try:
        resp = requests.post(uri, data=data, auth=auth, verify=False)
    except Exception:
        logging.exception('failed to post to %s', uri)
    else:
        if resp.ok:
            return

        else:
            logging.error('failed to post to %s, error=%s', uri, resp.text)
            raise


if __name__ == '__main__':

    indexer_hosts = ["10.141.71.69","10.141.65.122","10.141.71.74"]  # Add indexers here
    for indexer in indexer_hosts:
        create_token_with_ack(indexer)
        logging.info('token created for indexer:  %s', indexer)
        enable_ack_idle_cleanup(indexer)
        logging.info('AckIdleCleanup enabled for indexer:  %s', indexer)
        # TODO: add code to make sure calls were successful
