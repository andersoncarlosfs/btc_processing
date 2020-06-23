#!/usr/bin/python
# -*- coding: utf-8 -*-

# Connect to a websocket powered by blockchain.info and print events in the terminal in real time.

import argparse
import json
import websocket # install this with the following command: pip install websocket-client

from time import time

DEFAULT_LOGS=False
DEFAULT_START=True

class Generator(object):
    
    def __init__(self, logs=DEFAULT_LOGS, process=None):                
        self.logs = logs
        self.process = process        
        
    def start(self):
        ws = Generator.__open_websocket_to_blockchain()

        last_ping_time = time()

        while True:
            # Receive event
            data = json.loads(ws.recv())

            # We ping the server every 10s to show we are alive
            if time() - last_ping_time >= 10:
                ws.send(json.dumps({"op": "ping"}))
                last_ping_time = time()

            # Response to "ping" events
            if data["op"] == "pong":
                pass

            # New unconfirmed transactions
            elif data["op"] == "utx":
                transaction_timestamp = data["x"]["time"]
                transaction_hash = data['x']['hash'] # this uniquely identifies the transaction
                transaction_total_amount = 0

                for recipient in data["x"]["out"]:
                    # Every transaction may in fact have multiple recipients
                    # Note that the total amount is in hundredth of microbitcoin; you need to
                    # divide by 10**8 to obtain the value in bitcoins.
                    transaction_total_amount += recipient["value"] / 100000000.

                if self.logs:
                    print("{} New transaction {}: {} BTC".format(transaction_timestamp, transaction_hash, transaction_total_amount))

                if self.process:
                    self.process(json.dumps({
                        'timestamp': transaction_timestamp,
                        'hash': transaction_hash,
                        'total_amount': transaction_total_amount
                    }))


            # New block
            elif data["op"] == "block":
                block_hash = data['x']['hash']
                block_timestamp = data["x"]["time"]
                block_found_by = data["x"]["foundBy"]["description"]
                block_reward = 12.5 # blocks mined in 2016 have an associated reward of 12.5 BTC

                if self.logs:
                    print("{} New block {} found by {}".format(block_timestamp, block_hash, block_found_by))

                if self.process:
                    self.process(json.dumps({
                        'timestamp': block_timestamp,
                        'hash': block_hash,
                        'found_by': block_found_by
                    }))                    

            # This really should never happen
            else:
                if self.logs:
                    print("Unknown op: {}".format(data["op"]))

    @staticmethod
    def __open_websocket_to_blockchain():
        # Open a websocket
        ws = websocket.WebSocket()
        ws.connect("wss://ws.blockchain.info/inv")
        # Register to unconfirmed transaction events
        ws.send(json.dumps({"op":"unconfirmed_sub"}))
        # Register to block creation events
        ws.send(json.dumps({"op":"blocks_sub"}))

        return ws
            
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