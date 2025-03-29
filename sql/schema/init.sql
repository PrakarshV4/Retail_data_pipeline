-- STAR SCHEMA FOR RETAIL ANALYTICS

-- DIMENSIONS
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    quarter INT,
    year INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE dim_category (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(50)
);

CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category_id INT,
    price DECIMAL(10,2),
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    signup_date DATE,
    location_id INT,
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);

-- FACT TABLES
CREATE TABLE fact_transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    date_id DATE,
    amount DECIMAL(10,2),
    quantity INT,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_reviews (
    review_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    date_id DATE,
    rating INT,
    review_text TEXT,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- ANALYTICAL VIEWS
CREATE VIEW sales_performance AS
SELECT 
    p.product_id,
    p.product_name,
    c.category_name,
    COUNT(t.transaction_id) AS transaction_count,
    SUM(t.amount) AS total_revenue,
    AVG(r.rating) AS avg_rating
FROM dim_product p
JOIN dim_category c ON p.category_id = c.category_id
LEFT JOIN fact_transactions t ON p.product_id = t.product_id
LEFT JOIN fact_reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.product_name, c.category_name;
