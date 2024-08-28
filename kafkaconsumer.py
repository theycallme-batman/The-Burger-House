from kafka import KafkaConsumer
from json import dumps,loads
import hashlib
import time
from adls import ADLS_Connect

class Kafka_Consumer:
    def __init__(self,topic,bootstrap_servers):
        self.consumer = KafkaConsumer(topic,bootstrap_servers = bootstrap_servers,value_deserializer = lambda x: loads(x.decode('utf-8')))
        self.topic = topic
    def consume_and_upload(self,account_name,sas_token,batch_size,container):
        adls = ADLS_Connect(account_name,sas_token,container)
        try:
            currentbatch = []
            tries = 0
            while True:
                # Poll for new messages in batches 
                batch = self.consumer.poll(timeout_ms=1000, max_records=batch_size)
                print(f"polls - {tries}, current_batch_size {len(currentbatch)}")
                for tp, messages in batch.items():
                    print(f"Received message: {messages}")
                    for message in messages:
                        # Process each message in the batch
                        data = message.value
                        #print(f"Received message: {data}")
                        currentbatch.append(data)
                        
                if((len(currentbatch) > batch_size or tries > 2) and len(currentbatch) > 0):
                    directory_client = adls.get_directory(self.topic)
                    myfile = 'part-'+hashlib.sha256(str(time.time()).encode('utf-8')).hexdigest()[:15]+'.json'
                    file_client = directory_client.create_file(myfile)
                    records = "\n".join(currentbatch)
                    #for record in currentbatch:
                    filesize_previous = file_client.get_file_properties().size
                    print(filesize_previous)
                    file_client.append_data(records, offset=filesize_previous, length=len(records))
                    file_client.flush_data(filesize_previous+len(records))
                    currentbatch = []
                    tries = 0
                tries += 1
                time.sleep(15)

        except KeyboardInterrupt:
            print("Stopping the consumer...")
        finally:
            # Close the consumer connection
            self.consumer.close()
            