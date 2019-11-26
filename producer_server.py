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

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            lines = json.load(f)
            #print('lines:',lines)
            for line in lines: # originally f
                #print('line:',type(line), line)
                message = self.dict_to_binary(line)
                print('message:', message)
                # TODO send the correct data
                self.send(
                    topic=self.topic,
                    value=message
                    )
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        binary = json.dumps(json_dict).encode('utf-8')
        return binary 
        #out = io.BytesIO()
        #writer(out, data_stream.schema, json_dict )
        #return out.getValue()