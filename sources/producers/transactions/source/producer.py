#!flask/bin/python
# -*- coding: utf-8 -*-
import json

from kafka import KafkaProducer

from generator import DEFAULT_LOGS
from generator import DEFAULT_START
from generator import Generator
from generator import get_parser

DEFAULT_VOLUME='/var/lib/transactions/output'
DEFAULT_BROKERS=['0.0.0.0:9092', 'kafka:29092']
DEFAULT_RETRIES=0
DEFAULT_TOPIC='transactions'

# Creating a Producer
def new_producer(brokers=DEFAULT_BROKERS, retries=DEFAULT_RETRIES):
    return KafkaProducer(
        bootstrap_servers=brokers, 
        retries=retries, 
        value_serializer=lambda record: json.dumps(record).encode('utf-8')
    )    

# Getting a Producer from a request 
def get_producer(request):
    return new_producer(
        retries=request.args.get('retries', default=DEFAULT_RETRIES, type=int)
    )
    
# Sending a list of records as messages
def send_records(topic, messages=[], producer=new_producer(), on_send_success=None, on_send_error=None):
    for record in messages:
        future=producer.send(topic, record)
        
        # Assigning a callback success function  
        if on_send_success:
            future=future.add_callback(on_send_success)

        # Assigning a callback error function  
        if on_send_error:
            future=future.add_errback(on_send_error)

    # Locking until all asynchronous messages are sent
    producer.flush()   

if __name__ == '__main__':

    parser=get_parser()
    
    parser.add_argument('--brokers',
                        dest='brokers',
                        nargs='+',
                        help='Brokers for streaming'
                       )
    
    parser.add_argument('--retries',
                        dest='retries',
                        type=int,
                        help='Retries for streaming'
                       )
    
    parser.add_argument('--topic',
                        dest='topic',
                        type=str,
                        help='Topic for streaming'
                       )    

    parser.set_defaults(logs=DEFAULT_LOGS,
                        start=DEFAULT_START, 
                        brokers=DEFAULT_BROKERS,
                        retries=DEFAULT_RETRIES,
                        topic=DEFAULT_TOPIC
                       )
    
    arguments=parser.parse_args()

    generator = Generator(
        logs=arguments.logs,
        process=lambda record: send_records(
            arguments.topic, 
            messages=[record],
            producer=new_producer(
                brokers=arguments.brokers, 
                retries=arguments.retries
            ), 
            on_send_success=lambda metadata: print({
                'status':'success', 
                'topic':metadata.topic, 
                'partition':metadata.partition, 
                'offset':metadata.offset
            }), 
            on_send_error=lambda exception: print({
                'status':'error', 
                'error':exception
            }) 
        )
    )
   
    if arguments.start:
        generator.start()