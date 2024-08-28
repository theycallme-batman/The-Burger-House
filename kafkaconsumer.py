from kafka import KafkaConsumer
from json import dumps,loads
import hashlib
import time
from adls import ADLS_Connect

"""
Class - Kafka_Consumer
    This class connect to kafka as a consumer and pushed consumed data to ADLS .
"""

class Kafka_Consumer:
    def __init__(self,topic,bootstrap_servers):
        #Connecting to Kafka as a consumer with the server,deserializer, and topic details
        self.consumer = KafkaConsumer(topic,bootstrap_servers = bootstrap_servers,value_deserializer = lambda x: loads(x.decode('utf-8')))
        self.topic = topic

    def consume_and_upload(self,account_name,sas_token,batch_size,container):
        #Connecting to ADLS with the purpose of storing data in a data lake.
        adls = ADLS_Connect(account_name,sas_token,container)
        try:
            currentbatch = [] #will store the messages coming from each poll through kafka
            polls = 0 #polling counter 
            while True:
                # Poll for new messages in batches 
                batch = self.consumer.poll(timeout_ms=1000, max_records=batch_size)

                #Getting Topic partition and messages from the batch we polled
                for tp, messages in batch.items():
                    print(f" polls - {polls}, current_batch_size {len(currentbatch)} Received message: {messages}")
                    for message in messages:
                        #Extracting the value the value out of the message and appending to a list
                        #Purpose - We want the file to have multiple records based on the batch size
                        currentbatch.append(message.value)

                #Since we are storing the messages in a list and waiting to hit the batch_size, there could be chance where
                # there are no records coming in the next poll, in that case add condition to retry for 2 polls else create file
                # with the exisiting records present in currentbatch list        
                if((len(currentbatch) > batch_size or polls > 2) and len(currentbatch) > 0):
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
                    
                    #once the above conditions meet, start with the new batch
                    currentbatch = []
                    polls = 0

                #incrmented the polling counter
                polls += 1
                time.sleep(15)

        except KeyboardInterrupt:
            print("Stopping the consumer...")
        finally:
            # Close the consumer connection
            self.consumer.close()
            