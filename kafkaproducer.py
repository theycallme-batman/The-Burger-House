from kafka import KafkaProducer
from json import dumps,loads
from createdata import OrderDataGenerator
import time


class Kafka_Producer:
    def __init__(self,country, num_orders):
        # Generate fake order data
        self.order_data_generator = OrderDataGenerator(country, num_orders)
        
    def generateRecords(self):     
        #read the items and display
        maxItemID, itemDict = self.order_data_generator.readItems()
        self.order_data_generator.generate_order_data(maxItemID, itemDict)
        # Get DataFrames for orders and order items
        self.orders_df = self.order_data_generator.get_orders_dataframe()
        self.order_items_df = self.order_data_generator.get_order_items_dataframe()
        
    def connect_as_producer(self,bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers = bootstrap_servers,value_serializer = lambda x: dumps(x).encode('utf-8'))         
    
    def send_records_orders(self):
        
        for index, row in self.orders_df.iterrows():
            # Convert row to dictionary
            row_dict = row.to_dict()
    
            # Convert dictionary to JSON string
            row_json = dumps(row_dict)
            self.producer.send('orders',value = row_json)
            time.sleep(5)
        
    def send_records_items(self): 
        
        for index, row in self.order_items_df.iterrows():
            # Convert row to dictionary
            row_dict = row.to_dict()
    
            # Convert dictionary to JSON string
            row_json = dumps(row_dict)
            self.producer.send('orderitems',value = row_json)
            time.sleep(5)
        