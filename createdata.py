import random
import pandas as pd
from faker import Faker
from datetime import datetime , timedelta
from time import sleep
# Initialize Faker
fake = Faker()
Faker.seed(0)


"""
Class - OrderDataGenerator 
    This class generates fake order data and associated order items data for a specified country.
"""
class OrderDataGenerator:

    def __init__(self, country):
        #initialize variable for generating data
        self.country = country
        self.num_orders = 0
        self.orders_data = []
        self.order_items_data = []

    def generate_order_data(self,maxItemID, itemDict,num_orders): 
        self.orders_data = []
        self.order_items_data = []
        self.num_orders = num_orders
        for _ in range(self.num_orders):
            order_id = fake.uuid4()
            order_date = (datetime.now() + timedelta(minutes = random.randint(2, 10))).strftime('%Y-%m-%d %H:%M:%S')

            #customer_id = fake.uuid4()  --will be added in v2
            #customer_name = fake.name() --will be added in v2

            store_id = random.randint(1, 50)  # Assuming each country has up to 50 stores
            payment_method = random.choice(['Credit Card', 'Cash', 'Mobile Payment'])

            #order_status = random.choice(['Completed', 'Pending', 'Cancelled']) --will be added in v2

            # Generate multiple items for each order
            num_items = random.randint(1, 5)  # Random number of items per order
            total_order_amount = 0

            for _ in range(num_items):
                order_item_id = fake.uuid4()
                item_id = random.randint(1, maxItemID)  # Assuming 100 different items
                item_name = itemDict[item_id]['Item']
                quantity = random.randint(1, 3)
                unit_price = itemDict[item_id]['Price']
                total_price = round(quantity * unit_price, 2)

                #special_instructions = random.choice(['', 'Extra cheese', 'No onions', 'No ice'])  --will be added in v2
                #discount_applied = round(random.uniform(0.0, 1.0), 2)  --will be added in v2

                total_order_amount += total_price

                self.order_items_data.append({
                    'OrderItemID': order_item_id,
                    'OrderID': order_id,
                    'ItemID': item_id,
                    'ItemName': item_name,
                    'Quantity': quantity,
                    'UnitPrice': unit_price,
                    'TotalPrice': total_price,
                    #'SpecialInstructions': special_instructions, --will be added in v2
                    #'DiscountApplied': discount_applied, --will be added in v2
                })

            self.orders_data.append({
                'OrderID': order_id,
                'OrderDate': order_date,
                #'CustomerID': customer_id, --will be added in v2
                #'CustomerName': customer_name, --will be added in v2
                'StoreID': store_id,
                'GlobalID': self.country[:3] + "-" + str(store_id),
                'Country': self.country,
                'PaymentMethod': payment_method,
                'TotalAmount': total_order_amount,
                #'Status': order_status --will be added in v2
            })

    def get_orders_dataframe(self):
        return pd.DataFrame(self.orders_data) #Returns a DataFrame containing the generated order data.
    
    def get_order_items_dataframe(self):
        return pd.DataFrame(self.order_items_data) #Returns a DataFrame containing the generated order items data.

    def readItems(self):
        #Reads file for initdata and returning the same.
        df = pd.read_csv('./initdata/Items.csv')
        maxItemID = df["Id"].max()
        itemDict = df.set_index('Id').T.to_dict()
        return maxItemID,itemDict