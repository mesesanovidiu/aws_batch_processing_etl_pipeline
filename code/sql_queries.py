# sql_queries.py

create_sales_staging_table = """
    CREATE TABLE IF NOT EXISTS staging_sales (
        sales_sk INT IDENTITY(1,1) PRIMARY KEY ENCODE zstd,
        order_date DATE,
        ship_date DATE ENCODE zstd,
        order_number INT ENCODE zstd,
        order_line_number INT ENCODE zstd,
        product_code VARCHAR(10) ENCODE zstd,
        product_line VARCHAR(50) ENCODE zstd,
        suggested_retail_price INT ENCODE zstd,
        customer_id INT ENCODE zstd,
        customer_name VARCHAR(100) ENCODE zstd,
        city VARCHAR(50) ENCODE zstd,
        country VARCHAR(50) ENCODE zstd,
        territory VARCHAR(50) ENCODE zstd,
        contact_lastname VARCHAR(50) ENCODE zstd,
        contact_firstname VARCHAR(50) ENCODE zstd,
        quantity_ordered INT ENCODE zstd,
        price_each DECIMAL(10,2) ENCODE zstd,
        sales DECIMAL(10,2) ENCODE zstd,
        status VARCHAR(20) ENCODE zstd
    )
    DISTSTYLE KEY
    DISTKEY (customer_id)
    SORTKEY (sales_sk);
"""

create_table_dim_products = """
CREATE TABLE IF NOT EXISTS dim_products (
    product_PK INT IDENTITY(1,1) NOT NULL UNIQUE,
    product_code VARCHAR(10) NOT NULL ENCODE zstd,
    product_line VARCHAR(50) NOT NULL ENCODE zstd,
    suggested_retail_price INT NOT NULL ENCODE zstd,
    price_each DECIMAL(10,2) NOT NULL ENCODE zstd,
    start_date DATE NOT NULL ENCODE zstd,
    end_date DATE NOT NULL ENCODE zstd,
    is_current BOOLEAN NOT NULL DEFAULT true,
    CONSTRAINT pk_dim_products PRIMARY KEY (product_PK, end_date)
)
DISTSTYLE ALL
SORTKEY (product_PK);
"""

create_table_dim_customers = """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_PK INT IDENTITY(1,1) NOT NULL UNIQUE,
        customer_id INT NOT NULL ENCODE zstd,
        customer_name VARCHAR(100) NOT NULL ENCODE zstd,
        city VARCHAR(50) NOT NULL ENCODE zstd,
        country VARCHAR(50) NOT NULL ENCODE zstd,
        territory VARCHAR(50) NOT NULL ENCODE zstd,
        contact_lastname VARCHAR(50) NOT NULL ENCODE zstd,
        contact_firstname VARCHAR(50) NOT NULL ENCODE zstd,
        start_date DATE NOT NULL ENCODE zstd,
        end_date DATE NOT NULL ENCODE zstd,
        is_current BOOLEAN NOT NULL DEFAULT true,
        CONSTRAINT pk_dim_customers PRIMARY KEY (customer_PK, end_date)
    )
    DISTSTYLE ALL
    SORTKEY (customer_PK);
"""

create_table_dim_date = """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_PK INT PRIMARY KEY NOT NULL UNIQUE,
        date DATE NOT NULL ENCODE zstd,
        day_of_week INT NOT NULL ENCODE zstd,
        day_of_month INT NOT NULL ENCODE zstd,
        day_of_year INT NOT NULL ENCODE zstd,
        week_of_year INT NOT NULL ENCODE zstd,
        month INT NOT NULL ENCODE zstd,
        quarter INT NOT NULL ENCODE zstd,
        year INT NOT NULL ENCODE zstd,
        is_weekday BOOLEAN NOT NULL ENCODE zstd
    )
    DISTSTYLE ALL
    SORTKEY (date_PK);
"""


create_table_dim_status = """
    CREATE TABLE IF NOT EXISTS dim_status (
        status_PK INT IDENTITY(1,1) PRIMARY KEY,
        status varchar(20) ENCODE zstd
    )
    DISTSTYLE ALL
    SORTKEY (status_PK);
"""

update_dim_products = """
    UPDATE dim_products 
        SET is_current = false 
    FROM staging_sales 
    WHERE dim_products.product_code = staging_sales.product_code 
        AND dim_products.end_date = '9999-12-31' 
        AND dim_products.product_PK = (
    SELECT MAX(product_PK) 
    FROM dim_products 
    WHERE dim_products.product_code = staging_sales.product_code 
        AND dim_products.end_date = '9999-12-31'
);

INSERT INTO dim_products (product_code, product_line, suggested_retail_price, price_each, start_date, end_date, is_current)
SELECT DISTINCT
    ss.product_code,
    ss.product_line,
    ss.suggested_retail_price,
    ss.price_each,
    CURRENT_DATE,
    '9999-12-31'::date,
    true
FROM
    staging_sales ss
LEFT JOIN
    dim_products dp
ON
    dp.product_code = ss.product_code AND
    dp.product_line = ss.product_line AND
    dp.suggested_retail_price = ss.suggested_retail_price AND
    dp.price_each = ss.price_each AND
    dp.is_current = true
WHERE
    dp.product_code IS NULL;
"""


update_dim_customers = """
    UPDATE dim_customers 
    SET is_current = false 
    FROM staging_sales 
    WHERE dim_customers.customer_id = staging_sales.customer_id
        AND dim_customers.end_date = '9999-12-31' 
        AND dim_customers.customer_PK = (
    SELECT MAX(customer_PK) 
    FROM dim_customers 
    WHERE dim_customers.customer_id = staging_sales.customer_id
        AND dim_customers.end_date = '9999-12-31'
);

INSERT INTO dim_customers (customer_id, customer_name, city, country, territory, contact_lastname, contact_firstname, start_date, end_date, is_current)
SELECT DISTINCT
    ss.customer_id,
    ss.customer_name,
    ss.city,
    ss.country,
    CASE WHEN ss.territory = '' THEN 'not specified' ELSE ss.territory END as territory,
    ss.contact_lastname,
    ss.contact_firstname,
    CURRENT_DATE,
    '9999-12-31'::date,
    true
FROM
    staging_sales ss
LEFT JOIN
    dim_customers dc
ON
    dc.customer_id = ss.customer_id AND
    dc.customer_name = ss.customer_name AND
    dc.city = ss.city AND
    dc.country = ss.country AND
    CASE WHEN dc.territory = '' THEN 'not specified' ELSE dc.territory END = CASE WHEN ss.territory = '' THEN 'not specified' ELSE ss.territory END AND
    dc.contact_lastname = ss.contact_lastname AND
    dc.contact_firstname = ss.contact_firstname AND
    dc.is_current = true
WHERE
    dc.customer_id IS NULL;
"""


update_dim_date = """
    INSERT INTO dim_date
        WITH RECURSIVE
            start_dt AS (SELECT '2022-01-01'::date s_dt),
            end_dt AS (SELECT '2023-01-01'::date e_dt),
            cte_all_dates (dt) AS (
            SELECT s_dt dt FROM start_dt
            UNION ALL
            SELECT (dt + INTERVAL '1 day')::date dt
            FROM cte_all_dates
            WHERE dt <= (SELECT e_dt FROM end_dt)
    )
    SELECT
        EXTRACT(year from dt)*10000 + EXTRACT('month' from dt)*100 + EXTRACT('day' from dt) as date_PK,
        dt AS date,
        EXTRACT(DOW FROM dt) AS day_of_week,
        EXTRACT(DAY FROM dt) AS day_of_month,
        EXTRACT(DOY FROM dt) AS day_of_year,
        EXTRACT(WEEK FROM dt) AS week_of_year,
        EXTRACT(MONTH FROM dt) AS month,
        EXTRACT(QUARTER FROM dt) AS quarter,
        EXTRACT(YEAR FROM dt) AS year,
        CASE WHEN EXTRACT(DOW FROM dt) IN (0,6) THEN FALSE ELSE TRUE END AS is_weekday
    FROM cte_all_dates;
"""

update_dim_status = """
    INSERT INTO dim_status (status)
    SELECT DISTINCT status
    FROM staging_sales
    WHERE status NOT IN (SELECT status FROM dim_status);
"""

create_table_fact_sales = """
    CREATE TABLE fact_sales as
    SELECT 
        st.sales_sk,
        dd.date_PK as order_date_FK,
        dd2.date_PK as ship_date_FK,
        st.order_number as order_number_dd,
        st.order_line_number as order_line_number_dd,
        dc.customer_PK as customer_FK,
        dp.product_PK as product_FK,
		ds.status_PK as status_FK,
        st.quantity_ordered,
        st.sales
    FROM staging_sales st
    LEFT JOIN dim_date dd
        ON st.order_date = dd.date
    LEFT JOIN dim_date dd2
		ON st.ship_date = dd2.date
    LEFT JOIN dim_products dp
        ON st.product_code = dp.product_code
            AND st.product_line = dp.product_line
            AND st.suggested_retail_price = dp.suggested_retail_price
            AND st.price_each = dp.price_each
    LEFT JOIN dim_customers dc
        ON st.customer_id = dc.customer_id
            AND st.customer_name = dc.customer_name
            AND st.city = dc.city
            AND st.country = dc.country
            AND CASE WHEN st.territory = '' THEN 'not specified' ELSE st.territory END = dc.territory
            AND st.contact_lastname = dc.contact_lastname
            AND st.contact_firstname = dc.contact_firstname
	LEFT JOIN dim_status ds
		ON st.status = ds.status
"""

add_foreign_key_constraints = """
    ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_order_date
    FOREIGN KEY (order_date_FK)
    REFERENCES dim_date (date_PK);

    ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_ship_date
    FOREIGN KEY (ship_date_FK)
    REFERENCES dim_date (date_PK);

    ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_customer
    FOREIGN KEY (customer_FK)
    REFERENCES dim_customers (customer_PK);

    ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_status
    FOREIGN KEY (status_FK)
    REFERENCES dim_status (status_PK);
"""
