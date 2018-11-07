import argparse
import atexit
import json
import happybase
import logging
import time

from kafka import KafkaProducer

logger_format = "%(asctime)s - %(message)s"
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage-reader')
logger.setLevel(logging.DEBUG)

def shutdown_hook(producer, connection):
    """
    a shutdown hook to be called before the shutdown
    """
    try:
        logger.info('Closing Kafka consumer.')
        producer.flush(10)
        producer.close()
        logger.info('Kafka consumer closed.')
        logger.info('Closing Hbase connection.')
        connection.close()
        logger.info('Hbase connection closed.')
    except Exception as e:
        logger.warn('Failed to close consumer/connection, caused by: %s', str(e))
    finally:
        logger.info('Exiting program')


if __name__ == '__main__':
    # Setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic to subscribe from.')
    parser.add_argument('kafka_broker', help='the location of the kafka broker.')
    parser.add_argument('data_table', help='the data table to use.')
    parser.add_argument('hbase_host', help='the host name of hbase.')

    # Parse arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    data_table = args.data_table
    hbase_host = args.hbase_host

    # Initiate a simple kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # Initiate a hbase connection
    hbase_connection = happybase.Connection(hbase_host)

    # Setup preper shutdown hook
    atexit.register(shutdown_hook, kafka_producer, hbase_connection)

    # Exit if the table is not found
    hbase_tables = [table.decode() for table in hbase_connection.tables()]
    if data_table not in hbase_tables:
        logger.warn('%s is not found in hbase tables', str(data_table))
        exit()

    # Scan table and push to kafka
    table = hbase_connection.table(data_table)

    for key, data in table.scan():
        payload = {
            'Symbol':data[b'family:symbol'].decode(),
            'LastTradePrice':data[b'family:trade_price'].decode(),
            'Timestamp':data[b'family:trade_time'].decode()
        }

        logger.debug('Read data from hbase: %s', payload)
        kafka_producer.send(topic=topic_name, value=json.dumps(payload).encode('utf-8'))

        time.sleep(1)
