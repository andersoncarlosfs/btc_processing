#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json

from urllib import request

DEFAULT_LOGS=False
DEFAULT_START=True

class Generator(object):
    
    def __init__(self, logs=DEFAULT_LOGS, process=None):                
        self.logs = logs
        self.process = process        
        
    def start(self):
        while True:
            response = request.urlopen("http://api.coindesk.com/v1/bpi/currentprice.json")     
            
            data = json.loads(response.read())
            
            if data["chartName"] == "Bitcoin":
                time = data["time"]    
                rates = data["bpi"]
                             
                if self.logs:
                    print("{} New rate: {} EUR".format(time["updatedISO"], rates["EUR"]["rate"]))

                if self.process:
		    self.process(json.dumps(data))
            else:
                if self.logs:
                    print("Unknown chart: {}".format(data["chartName"]))                    

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
