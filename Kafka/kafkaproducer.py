from kafka import KafkaProducer
from json import dumps
from Generator.createdata import OrderDataGenerator

"""
Class - Kafka_Producer
    This class creates an object of order_data_generator , connect to kafka as a producer and sends the generated records to kafka server.
"""
class Kafka_Producer:
    def __init__(self,country):
        #Initialiaze the Data Generator object
        self.order_data_generator = OrderDataGenerator(country)
        
        
    def generateRecords(self,num_orders):     
        #Get the initial item data information from the ./initdata 
        maxItemID, itemDict = self.order_data_generator.readItems()

        #Calling the generate_order_data for generating the orders
        self.order_data_generator.generate_order_data(maxItemID, itemDict,num_orders)

        #Get DataFrames for orders and order items
        self.orders_df = self.order_data_generator.get_orders_dataframe()
        self.order_items_df = self.order_data_generator.get_order_items_dataframe()

        
    def connect_as_producer(self,bootstrap_servers):
        #Connecting to Kafka as a producer with the server and serializer details
        self.producer = KafkaProducer(bootstrap_servers = bootstrap_servers,value_serializer = lambda x: dumps(x).encode('utf-8'))         
    
    def send_records_orders(self):
        
        #End goal is to convert data into json, iterating every record is to simulate streaming data
        for index, row in self.orders_df.iterrows():
            #Convert row to dictionary
            row_dict = row.to_dict()
    
            #Convert dictionary to JSON string
            row_json = dumps(row_dict)

            #Send data to Kafka
            self.producer.send('orders',value = row_json)

            
        
    def send_records_items(self): 
        
        #End goal is to convert data into json, iterating every record is to simulate streaming data
        for index, row in self.order_items_df.iterrows():
            # Convert row to dictionary
            row_dict = row.to_dict()
    
            # Convert dictionary to JSON string
            row_json = dumps(row_dict)

            #Send data to Kafka
            self.producer.send('orderitems',value = row_json)

            
        