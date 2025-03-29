from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import os
import numpy as np

fake = Faker()

# Configuration
NUM_CUSTOMERS = 2000
NUM_PRODUCTS = 300
NUM_TRANSACTIONS = 30000
NUM_REVIEWS = 7500
START_DATE = datetime(2021, 1, 1)
END_DATE = datetime(2023, 1, 1)


def generate_dimensions():
    # Locations
    locations = [(i, fake.city(), fake.state(), fake.country())
                 for i in range(1, 501)]
    location_df = pd.DataFrame(locations, columns=['location_id', 'city', 'state', 'country'])
    location_df.to_csv('data/raw/dim_location.csv', index=False)

    # Categories
    categories = [
        (1, 'Electronics'), (2, 'Clothing'),
        (3, 'Home Appliances'), (4, 'Books'),
        (5, 'Sports'), (6, 'Beauty'),
        (7, 'Toys'), (8, 'Groceries')
    ]
    category_df = pd.DataFrame(categories, columns=['category_id', 'category_name'])
    category_df.to_csv('data/raw/dim_category.csv', index=False)

    # Products
    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        products.append((
            i,
            fake.word().title() + " " + fake.word().title(),
            random.choice(categories)[0],
            round(random.uniform(5, 500), 2)
        ))

    # Introduce NaN values randomly into the `product_name` column
    product_df = pd.DataFrame(products, columns=['product_id', 'product_name', 'category_id', 'price'])
    product_df['product_name'] = product_df['product_name'].apply(
        lambda x: x if random.random() > 0.05 else None
    )

    # Introduce some duplicate products to simulate data issues
    product_df = pd.concat([product_df, product_df.sample(10)], ignore_index=True)

    # Introduce an outlier in the price column (e.g., price > 1000)
    product_df.loc[product_df.sample(5).index, 'price'] = random.randint(1000, 5000)

    product_df.to_csv('data/raw/dim_product.csv', index=False)

    # Customers
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customers.append((
            i,
            fake.name(),
            fake.email(),
            fake.date_between(START_DATE, END_DATE),
            random.choice(locations)[0]
        ))

    customer_df = pd.DataFrame(customers, columns=['customer_id', 'name', 'email', 'signup_date', 'location_id'])

    # Introduce NaN values randomly into the `email` and `signup_date` columns
    customer_df['email'] = customer_df['email'].apply(
        lambda x: x if random.random() > 0.05 else None
    )
    customer_df['signup_date'] = customer_df['signup_date'].apply(
        lambda x: x if random.random() > 0.05 else None
    )

    # Introduce duplicate customers to simulate data issues
    customer_df = pd.concat([customer_df, customer_df.sample(5)], ignore_index=True)

    customer_df.to_csv('data/raw/dim_customer.csv', index=False)
def generate_facts():
    # Date dimension
    dates = []
    current_date = START_DATE
    while current_date <= END_DATE:
        dates.append((
            current_date.date(),
            current_date.day,
            current_date.month,
            (current_date.month - 1) // 3 + 1,
            current_date.year,
            current_date.weekday() + 1,
            current_date.weekday() >= 5
        ))
        current_date += timedelta(days=1)

    date_df = pd.DataFrame(dates, columns=['date_id', 'day', 'month', 'quarter', 'year', 'day_of_week', 'is_weekend'])
    date_df.to_csv('data/raw/dim_date.csv', index=False)

    # Transactions
    transactions = []
    products = pd.read_csv('data/raw/dim_product.csv')
    for i in range(1, NUM_TRANSACTIONS + 1):
        product = products.sample(1).iloc[0]
        transactions.append((
            i,
            random.randint(1, NUM_CUSTOMERS),
            product['product_id'],
            fake.date_between(START_DATE, END_DATE).isoformat(),
            round(product['price'] * random.uniform(0.8, 1.2) * random.randint(1, 5), 2),
            random.randint(1, 5)
        ))

    transaction_df = pd.DataFrame(transactions,
                                  columns=['transaction_id', 'customer_id', 'product_id', 'date_id', 'amount',
                                           'quantity'])

    # Introduce NaN values randomly into the `amount` column
    transaction_df['amount'] = transaction_df['amount'].apply(
        lambda x: x if random.random() > 0.05 else None
    )

    # Introduce duplicate transactions to simulate data issues
    transaction_df = pd.concat([transaction_df, transaction_df.sample(10)], ignore_index=True)

    # Introduce outliers in the `amount` column (e.g., extremely high values)
    transaction_df.loc[transaction_df.sample(5).index, 'amount'] = random.randint(1000, 10000)

    transaction_df.to_csv('data/raw/fact_transactions.csv', index=False)

    # Reviews
    reviews = []
    for i in range(1, NUM_REVIEWS + 1):
        transaction = random.choice(transactions)
        reviews.append((
            i,
            transaction[1],  # customer_id
            transaction[2],  # product_id
            (datetime.strptime(transaction[3], '%Y-%m-%d') + timedelta(days=random.randint(1, 30))).date(),
            random.randint(1, 5),
            fake.paragraph()
        ))

    review_df = pd.DataFrame(reviews,
                             columns=['review_id', 'customer_id', 'product_id', 'date_id', 'rating', 'review_text'])

    # Introduce NaN values randomly into the `review_text` column
    review_df['review_text'] = review_df['review_text'].apply(
        lambda x: x if random.random() > 0.05 else None
    )

    # Introduce duplicate reviews to simulate data issues
    review_df = pd.concat([review_df, review_df.sample(5)], ignore_index=True)

    review_df.to_csv('data/raw/fact_reviews.csv', index=False)


if __name__ == "__main__":
    os.makedirs('data/raw', exist_ok=True)
    generate_dimensions()
    generate_facts()
    print("Data generation complete! Files saved to data/raw/")
