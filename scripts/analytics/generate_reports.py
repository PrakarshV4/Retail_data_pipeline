import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def create_reports():
    # Connect to MySQL
    conn = mysql.connector.connect(
        host="mysql-dw",
        user="retail_user",
        password="retailpass",
        database="retail_dw"
    )
    
    # Ensure reports directory exists
    os.makedirs('/opt/airflow/reports', exist_ok=True)

    # 1. Sales Performance Report
    sales_df = pd.read_sql("""
        SELECT product_name, category_name, 
               SUM(amount) as revenue, 
               COUNT(*) as transactions
        FROM fact_transactions t
        JOIN dim_product p ON t.product_id = p.product_id
        JOIN dim_category c ON p.category_id = c.category_id
        GROUP BY product_name, category_name
        ORDER BY revenue DESC
        LIMIT 20
    """, conn)

    plt.figure(figsize=(12, 8))
    sns.barplot(data=sales_df, x='revenue', y='product_name', hue='category_name')
    plt.title('Top 20 Products by Revenue')
    plt.tight_layout()
    plt.savefig('/opt/airflow/reports/top_products.png')
    plt.close()

    # 2. Customer Segmentation
    customers_df = pd.read_sql("""
        SELECT 
            CASE 
                WHEN total_spent > 5000 THEN 'VIP'
                WHEN total_spent > 1000 THEN 'Premium'
                ELSE 'Regular'
            END as segment,
            COUNT(*) as customers,
            SUM(total_spent) as revenue
        FROM (
            SELECT customer_id, SUM(amount) as total_spent
            FROM fact_transactions
            GROUP BY customer_id
        ) t
        GROUP BY segment
    """, conn)

    plt.figure(figsize=(10, 6))
    customers_df.set_index('segment')['revenue'].plot.pie(autopct='%1.1f%%')
    plt.title('Revenue by Customer Segment')
    plt.savefig('/opt/airflow/reports/customer_segments.png')
    plt.close()

    conn.close()

if __name__ == "__main__":
    create_reports()
    print("Reports generated successfully!")
