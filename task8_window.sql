SET GLOBAL local_infile = 1;
USE INTERNSHIP;
DROP TABLE IF EXISTS superstore;

CREATE TABLE superstore72 (
    row_id VARCHAR(20),
    order_id VARCHAR(50),
    order_date VARCHAR(20),
    ship_date VARCHAR(20),
    ship_mode VARCHAR(50),
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    market VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    sales VARCHAR(20),
    quantity VARCHAR(20),
    discount VARCHAR(20),
    profit VARCHAR(20),
    shipping_cost VARCHAR(20),
    order_priority VARCHAR(20)
);
LOAD DATA LOCAL INFILE '\C:\data analyst_internship\Superstore-Global_Dataset.csv/'
INTO TABLE superstore
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
USE internship;
SELECT COUNT(*) FROM superstore72;
SELECT * FROM superstore LIMIT 10;
SELECT order_date FROM superstore72 LIMIT 5;
SELECT
    customer_name,
    region,
    SUM(sales) AS total_sales,
    DENSE_RANK() OVER (
        PARTITION BY region
        ORDER BY SUM(sales) DESC
    ) AS rank_in_region
FROM superstore
GROUP BY customer_name, region;
SELECT
    order_date,
    sales,
    SUM(sales) OVER (
        ORDER BY order_date
    ) AS running_total
FROM superstore72;
WITH monthly_sales AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m-01') AS month,
        SUM(sales) AS total_sales
    FROM superstore
    GROUP BY month
)
SELECT
    month,
    total_sales,
    LAG(total_sales) OVER (ORDER BY month) AS prev_month,
    total_sales - LAG(total_sales) OVER (ORDER BY month) AS mom_growth
FROM monthly_sales;
WITH product_sales AS (
    SELECT
        category,
        product_name,
        SUM(sales) AS total_sales
    FROM superstore
    GROUP BY category, product_name
),
ranked_products AS (
    SELECT *,
           DENSE_RANK() OVER (
               PARTITION BY category
               ORDER BY total_sales DESC
           ) AS rnk
    FROM product_sales
)
SELECT *
FROM ranked_products
WHERE rnk <= 3;
