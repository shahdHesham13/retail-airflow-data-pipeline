import pandas as pd
from faker import Faker
import random
from uuid import uuid4
import os

faker_instance = Faker()

def generate_customers(num_customers=100, data_folder='/opt/airflow/data'):
    customer_data = []
    for _ in range(num_customers):
        customer = {
            'customer_id': str(uuid4()),
            'first_name': faker_instance.first_name(),
            'last_name': faker_instance.last_name(),
            'email': faker_instance.email(),
            'address': faker_instance.address(),
            'city': faker_instance.city(),
            'state': faker_instance.state(),
            'country': faker_instance.country(),
            'postal_code': faker_instance.postcode(),
            'date_of_birth': faker_instance.date_of_birth(),
            'account_balance': round(random.uniform(100, 10000), 2),
            'created_at': faker_instance.date_time_this_year()
        }
        customer_data.append(customer)
    df_customers = pd.DataFrame(customer_data)
    df_customers.to_csv(os.path.join(data_folder, 'customers.csv'), index=False)

def generate_products(num_products=50, data_folder='/opt/airflow/data'):
    product_data = []
    for i in range(num_products):
        product = {
            'product_id': i + 1,
            'product_name': faker_instance.word(),
            'category': random.choice(['Makeup', 'Blouses', 'Skirts', 'Dresses', 'Shoes', 'accessories', 'Bags']),
            'price': round(random.uniform(5, 500), 2),
            'stock_quantity': random.randint(1, 100)
        }
        product_data.append(product)
    df_products = pd.DataFrame(product_data)
    df_products.to_csv(os.path.join(data_folder, 'products.csv'), index=False)

def generate_stores(num_stores=10, data_folder='/opt/airflow/data'):
    store_data = []
    for i in range(num_stores):
        store = {
            'store_id': i + 1,
            'store_name': faker_instance.company(),
            'location': faker_instance.city(),
            'store_type': random.choice(['Online', 'Physical']),
        }
        store_data.append(store)
    df_stores = pd.DataFrame(store_data)
    df_stores.to_csv(os.path.join(data_folder, 'stores.csv'), index=False)

def generate_transactions(num_transactions=1000, data_folder='/opt/airflow/data'):
    df_customers = pd.read_csv(os.path.join(data_folder, 'customers.csv'))
    df_products = pd.read_csv(os.path.join(data_folder, 'products.csv'))
    df_stores = pd.read_csv(os.path.join(data_folder, 'stores.csv'))
    
    transaction_data = []
    for _ in range(num_transactions):
        transaction = {
            'transaction_id': str(uuid4()),
            'customer_id': random.choice(df_customers['customer_id']),
            'transaction_date': faker_instance.date_time_this_year(),
            'transaction_amount': round(random.uniform(20, 1000), 2),
            'store_id': random.choice(df_stores['store_id']),
            'product_id': random.choice(df_products['product_id']),
            'quantity': random.randint(1, 10),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'Cash', 'PayPal'])
        }
        transaction_data.append(transaction)
    df_transactions = pd.DataFrame(transaction_data)
    df_transactions.to_csv(os.path.join(data_folder, 'transactions.csv'), index=False)