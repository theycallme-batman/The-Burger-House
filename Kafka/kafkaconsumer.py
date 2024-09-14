from kafka import KafkaConsumer
from json import loads
import hashlib
import time
from Connectors.ADLS.adls import ADLS_Connect

"""
Class - Kafka_Consumer
    This class connect to kafka as a consumer and pushed consumed data to ADLS .
"""

class Kafka_Consumer:
    def __init__(self,topic,bootstrap_servers,groupID):
        #Connecting to Kafka as a consumer with the server,deserializer, and topic details
        self.consumer = KafkaConsumer(topic
                                      ,bootstrap_servers = bootstrap_servers
                                      ,value_deserializer = lambda x: loads(x.decode('utf-8'))
                                      ,group_id = groupID
                                      ,auto_offset_reset = "latest"
                                      )
        self.topic = topic

    def consume_and_upload(self,account_name,sas_token,batch_size,container):
        #Connecting to ADLS with the purpose of storing data in a data lake.
        adls = ADLS_Connect(account_name,sas_token,container)
        try:
            while True:
                # Poll for new messages in batches 
                batch = self.consumer.poll(timeout_ms= 15000, max_records=batch_size)

                if batch is None:
                    continue
                else:

                    currentbatch = [] #will store the messages coming from each poll through kafka
                    
                    #Getting Topic partition and messages from the batch we polled
                    for tp, messages in batch.items():
                        print(f"current_batch_size {len(currentbatch)} Received message: {messages}")
                        for message in messages:
                            #Extracting the value the value out of the message and appending to a list
                            #Purpose - We want the file to have multiple records based on the batch size
                            currentbatch.append(message.value)


                    directory_client = adls.get_directory(self.topic)

                    #file name is generated based on the time its been created
                    myfile = 'part-'+hashlib.sha256(str(time.time()).encode('utf-8')).hexdigest()[:15]+'.json'
                    file_client = directory_client.create_file(myfile)

                    #formatting the records
                    records = "\n".join(currentbatch)

                    filesize_previous = file_client.get_file_properties().size

                    #appending the saving the file
                    file_client.append_data(records, offset=filesize_previous, length=len(records))
                    file_client.flush_data(filesize_previous+len(records))

        except KeyboardInterrupt:
            print("Stopping the consumer...")
        finally:
            # Close the consumer connection
            self.consumer.close()
            