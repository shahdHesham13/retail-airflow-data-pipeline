import os
import pandas as pd

def calculate_metrics(data_folder='/opt/airflow/data'):
    transactions_file = os.path.join(data_folder, 'transactions.csv')
    df_transactions = pd.read_csv(transactions_file)

    if df_transactions.empty:
        return os.path.join(data_folder, 'metrics_report.txt')

    total_transaction_amount = df_transactions['transaction_amount'].sum()
    total_unique_customers = df_transactions['customer_id'].nunique()
    top_customer_df = df_transactions.groupby('customer_id')['transaction_amount'].sum()
    top_customer = top_customer_df.idxmax() if not top_customer_df.empty else 'N/A'
    top_customer_amount = top_customer_df.max() if not top_customer_df.empty else 0
    average_transaction_amount = df_transactions['transaction_amount'].mean()
    total_number_of_transactions = df_transactions['transaction_id'].nunique()
    min_transaction_amount = df_transactions['transaction_amount'].min()
    max_transaction_amount = df_transactions['transaction_amount'].max()
    most_common_transaction_amount = df_transactions['transaction_amount'].mode()[0] if not df_transactions['transaction_amount'].mode().empty else 'N/A'
    transaction_count_per_customer = df_transactions.groupby('customer_id')['transaction_id'].count()
    most_frequent_customer = transaction_count_per_customer.idxmax() if not transaction_count_per_customer.empty else 'N/A'
    most_frequent_customer_count = transaction_count_per_customer.max() if not transaction_count_per_customer.empty else 0
    
    report = f"""
    Transaction Metrics Report
    ==========================
    Total Transaction Amount: {total_transaction_amount:.2f}
    Total Unique Customers: {total_unique_customers}
    Top Customer ID: {top_customer} (Total: {top_customer_amount:.2f})
    Most Frequent Customer ID: {most_frequent_customer} (Transactions: {most_frequent_customer_count})
    Average Transaction Amount: {average_transaction_amount:.2f}
    Most Common Transaction Amount: {most_common_transaction_amount:.2f}
    Total Number of Transactions: {total_number_of_transactions}
    Minimum Transaction Amount: {min_transaction_amount:.2f}
    Maximum Transaction Amount: {max_transaction_amount:.2f}
    """

    report_file = os.path.join(data_folder, 'metrics_report.txt')
    with open(report_file, 'w') as f:
        f.write(report)

    return report_file
