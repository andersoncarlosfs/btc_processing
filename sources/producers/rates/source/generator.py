#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json

from urllib import request
from datetime import datetime

DEFAULT_LOGS=False
DEFAULT_START=True

class Generator(object):
    
    def __init__(self, logs=DEFAULT_LOGS, process=None):                
        self.logs = logs
        self.process = process        
        
    def start(self):
        while True:
            try:
                response = request.urlopen("http://api.coindesk.com/v1/bpi/currentprice.json")     

                data = json.loads(response.read())

                if data["chartName"] == "Bitcoin":
                    time = data["time"]    
                    rates = data["bpi"]

                    if self.logs:
                        print("{} New rate: {} EUR".format(time["updated"], rates["EUR"]["rate"]))

                    if self.process:
                        self.process(json.dumps({                            
                            'timestamp': time['updatedISO'], #'timestamp': datetime.fromisoformat(time['updatedISO']).timestamp(), #'timestamp': datetime.strptime(time['updatedISO'], "%Y-%m-%dT%H:%M:%S%z").timestamp(),
                            'rate': rates["EUR"]["rate_float"]
                        }))
            
                else:
                    if self.logs:
                        print("Unknown chart: {}".format(data["chartName"]))  

            except Exception as e:
                if self.logs:
                    print("Error: {} ".format(e))                  

def get_parser():
    parser = argparse.ArgumentParser(description='Data generator')
    
    parser.add_argument('--start',
                        dest='start',
                        type=bool,
                        help='Start the data generator')
    
    parser.add_argument('--logs',
                        dest='logs',
                        type=bool,
                        help='Logs of the data generator')

    parser.set_defaults(start=DEFAULT_START, logs=DEFAULT_LOGS)
    
    return parser
    
if __name__ == "__main__":

    args = get_parser().parse_args()

    generator = Generator(logs=args.logs)
    
    if args.start:
        generator.start()
