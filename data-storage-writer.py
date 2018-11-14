import argparse
import atexit
import json
import logging
import happybase

from kafka import KafkaConsumer

logger_format = "%(asctime)s - %(message)s"
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage-writer')
logger.setLevel(logging.DEBUG)

def shutdown_hook(consumer, connection):
    """
    a shutdown hook to be called before the shutdown
    """
    try:
        logger.info('Closing Kafka consumer.')
        consumer.close()
        logger.info('Kafka consumer closed.')
        logger.info('Closing Hbase connection.')
        connection.close()
        logger.info('Hbase connection closed.')
    except Exception as e:
        logger.warn('Failed to close consumer/connection, caused by: %s', str(e))
    finally:
        logger.info('Exiting program')


def persist_data(data, hbase_connection, data_table):
    """
    Presist data into hbase
    """
    try:
        logger.debug('Start to presist data to hbase: %s', data)
        parsed = json.loads(data)
        symbol = parsed.get('Symbol')
        price = float(parsed.get('LastTradePrice'))
        timestamp = parsed.get('Timestamp')

        table = hbase_connection.table(data_table)
        row_key = "%s-%s" % (symbol, timestamp)
        logger.info('Storing values with row key %s' % row_key)
        table.put(row_key, {'family:symbol': str(symbol),
                            'family:trade_time': str(timestamp),
                            'family:trade_price': str(price)})

        logger.info('Persisted data to hbase for symbol: %s, price: %s, timestamp: %s', symbol, price, timestamp)

    except Exception as e:
        logger.error('Failed to presist data to hbase for %s', str(e))


if __name__ == '__main__':
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

    # Initiate a simple kafka consumer
    kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

    # Initiate a hbase connection
    hbase_connection = happybase.Connection(hbase_host)

    # Create table if not exists
    hbase_tables = [table.decode() for table in hbase_connection.tables()]
    if data_table not in hbase_tables:
        hbase_connection.create_table(data_table, {'family': dict()})

    # Setup proper shutdownn hook
    atexit.register(shutdown_hook, kafka_consumer, hbase_connection)

    # Start cosuming kafka and writing to hbase
    for message in kafka_consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        persist_data(message.value, hbase_connection, data_table)
