import random
import pandas as pd
from faker import Faker
from datetime import datetime

# Initialize Faker
fake = Faker()
Faker.seed(0)

class OrderDataGenerator:
    """
    This class generates fake order data and associated order items data for a specified country.
    """
    def __init__(self, country, num_orders):
        self.country = country
        self.num_orders = num_orders
        self.orders_data = []
        self.order_items_data = []

    def generate_order_data(self,maxItemID, itemDict):
        """
        Generates fake order data and stores it in a DataFrame.
        """
        for _ in range(self.num_orders):
            order_id = fake.uuid4()
            order_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            #customer_id = fake.uuid4()
            #customer_name = fake.name()
            store_id = random.randint(1, 50)  # Assuming each country has up to 50 stores
            payment_method = random.choice(['Credit Card', 'Cash', 'Mobile Payment'])
            #order_status = random.choice(['Completed', 'Pending', 'Cancelled'])

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
                #special_instructions = random.choice(['', 'Extra cheese', 'No onions', 'No ice'])
                #discount_applied = round(random.uniform(0.0, 1.0), 2)
                #calories = random.randint(200, 800)

                total_order_amount += total_price

                self.order_items_data.append({
                    'OrderItemID': order_item_id,
                    'OrderID': order_id,
                    'ItemID': item_id,
                    'ItemName': item_name,
                    'Quantity': quantity,
                    'UnitPrice': unit_price,
                    'TotalPrice': total_price,
                    #'SpecialInstructions': special_instructions,
                    #'DiscountApplied': discount_applied,
                    #'Calories': calories
                })

            self.orders_data.append({
                'OrderID': order_id,
                'OrderDate': order_date,
                #'CustomerID': customer_id,
                #'CustomerName': customer_name,
                'StoreID': store_id,
                'Country': self.country,
                'PaymentMethod': payment_method,
                'TotalAmount': total_order_amount,
                #'Status': order_status
            })

    def get_orders_dataframe(self):
        """
        Returns a DataFrame containing the generated order data.
        """
        return pd.DataFrame(self.orders_data)

    def readItems(self):
        df = pd.read_csv('./initdata/Items.csv')
        maxItemID = df["Id"].max()
        itemDict = df.set_index('Id').T.to_dict()
        #print(itemDict)
        return maxItemID,itemDict
    
    
    def get_order_items_dataframe(self):
        """
        Returns a DataFrame containing the generated order items data.
        """
        return pd.DataFrame(self.order_items_data)