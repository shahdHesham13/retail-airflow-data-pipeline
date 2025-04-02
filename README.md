
# Airflow Data Pipeline
**End-to-End ETL Pipeline with PostgreSQL & Automated Reporting**  

![pipelineg](https://imgur.com/wyaIthH)

## Overview
This Airflow DAG automates the generation of synthetic commerce data (customers, products, stores, transactions), loads it into PostgreSQL, calculates business metrics, and emails a valuable business insights report.

## Key Features & Technologies
This pipeline creates a seamless, production-grade workflow.

- **Orchestration:** Managed by **Apache Airflow**
- **Structured Data Storage:** Uses **PostgreSQL** with proper constraints (FKs, PKs)
- **Synthetic Data:** Generated and transformed with **Python** (Pandas, Faker)
- **Automated Reporting:** Sends daily reports via **Airflow’s EmailOperator**
- **Self-Cleaning:** Deletes temporary files after execution

## Pipeline Steps

1. **Create Tables**  
   - `customers`, `products`, `stores`, `transactions` in PostgreSQL  
   - Must create `customers/products/stores` before `transactions`

1. **Generate Data using the `Faker` Python package**
   - 100 customers, 50 products, 10 stores, 1,000 transactions (configurable)  

1. **Calculate Metrics on the generated transaction data**  
   ```python
   # Example: Top customer by spend
   df_transactions.groupby('customer_id')['amount'].sum().nlargest(1)
   ```

4. **Email Report**  
   - Sent as a `.txt` file with the calculated key metrics  

5. **Cleanup**  
   - Deletes all generated CSV/temp files  

![acpipeline](https://imgur.com/CFz6SZl)
