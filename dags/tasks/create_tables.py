import psycopg2

def create_tables():
    conn = psycopg2.connect("dbname=airflow user=airflow password=airflow host=postgres")
    cursor = conn.cursor()
    
    # Create customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id UUID PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            address TEXT,
            city VARCHAR(50),
            state VARCHAR(50),
            country VARCHAR(50),
            postal_code VARCHAR(20),
            date_of_birth DATE,
            account_balance NUMERIC(10, 2),
            created_at TIMESTAMP
        );
    """)
    
    # Create products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(100),
            category VARCHAR(50),
            price NUMERIC(10, 2),
            stock_quantity INT
        );
    """)
    
    # Create stores table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id SERIAL PRIMARY KEY,
            store_name VARCHAR(100),
            location VARCHAR(100),
            store_type VARCHAR(50)
        );
    """)
    
    # Create transactions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id UUID PRIMARY KEY,
            customer_id UUID REFERENCES customers(customer_id),
            transaction_date TIMESTAMP,
            transaction_amount NUMERIC(10, 2),
            store_id INT REFERENCES stores(store_id),
            product_id INT REFERENCES products(product_id),
            quantity INT,
            payment_method VARCHAR(50)
        );
    """)
    
    conn.commit()
    cursor.close()
    conn.close()