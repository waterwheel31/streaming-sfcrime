from kafka import KafkaProducer
import json
import time
import io
from fastavro import parse_schema, writer
import data_stream


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

        print('input file:', input_file)

    def generate_data(self):
        with open(self.input_file) as f:
            lines = json.load(f)

            for line in lines: # originally f
              
                message = self.dict_to_binary(line)
                print('message:', message)
                
                self.send(
                    topic=self.topic,
                    value=message
                    )
                time.sleep(1)

  
    def dict_to_binary(self, json_dict):
        binary = json.dumps(json_dict).encode('utf-8')
        return binary 
     