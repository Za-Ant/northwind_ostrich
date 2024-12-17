CREATE OR REPLACE DATABASE northwind_ostrich;
CREATE OR REPLACE STAGE ostrich_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE products_staging (
    id INT,
    productName STRING,
    supplierId INT,
    categoryId INT,
    unit STRING,
    price INT
);

CREATE OR REPLACE TABLE orders_staging (
    id INT,
    customerId INT,
    employeeId INT,
    orderDate STRING,
    shipperId INT
);

CREATE OR REPLACE TABLE customers_staging (
    id INT,
    customerName STRING,
    contactName STRING,
    address STRING,
    city STRING,
    postalCode STRING,
    country STRING
);

CREATE OR REPLACE TABLE categories_staging (
    id INT,
    categoryName STRING,
    description STRING
);

CREATE OR REPLACE TABLE employees_staging (
    id INT,
    lastName STRING,
    firstName STRING,
    birthDate STRING,
    photo STRING,
    notes STRING
);

CREATE OR REPLACE TABLE orderdetails_staging (
    id INT,
    orderId STRING,
    productId STRING,
    quantity INT
);

CREATE OR REPLACE TABLE shippers_staging (
    id INT,
    shipperName STRING,
    phone STRING
);

CREATE OR REPLACE TABLE suppliers_staging (
    id INT,
    supplierName STRING,
    contactName STRING,
    address STRING,
    city STRING,
    postalCode STRING,
    country STRING,
    phone STRING  
);


COPY INTO products_staging
FROM @ostrich_stage/products.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO orders_staging
FROM @ostrich_stage/orders.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO customers_staging
FROM @ostrich_stage/customers.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO categories_staging
FROM @ostrich_stage/categories.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employees_staging
FROM @ostrich_stage/employees.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO orderdetails_staging
FROM @ostrich_stage/orderdetails.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO shippers_staging
FROM @ostrich_stage/shippers.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO suppliers_staging
FROM @ostrich_stage/suppliers.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);



SELECT * FROM products_staging;
SELECT * FROM orders_staging;
SELECT * FROM customers_staging;
SELECT * FROM categories_staging;
SELECT * FROM employees_staging;
SELECT * FROM orderdetails_staging;
SELECT * FROM shippers_staging;
SELECT * FROM suppliers_staging;

CREATE OR REPLACE TABLE dim_addresses (
    address_id INT AUTOINCREMENT PRIMARY KEY,
    address STRING,
    city STRING,
    postalCode STRING,
    country STRING
);

INSERT INTO dim_addresses (address, city, postalCode, country)
SELECT DISTINCT address, city, postalCode, country
FROM (
    SELECT address, city, postalCode, country FROM suppliers_staging
    UNION
    SELECT address, city, postalCode, country FROM customers_staging
);

SELECT * FROM dim_addresses;


CREATE OR REPLACE TABLE dim_customers AS 
SELECT
    c.id AS customer_id,
    c.customername AS name,
    a.address_id
FROM customers_staging c
JOIN dim_addresses a ON c.address = a.address
   AND c.city = a.city
   AND c.postalCode = a.postalCode
   AND c.country = a.country;

SELECT * FROM dim_customers;

CREATE OR REPLACE TABLE dim_suppliers AS 
SELECT
    s.id AS supplier_id,
    s.suppliername AS name,
    a.address_id
FROM suppliers_staging s
JOIN dim_addresses a ON s.address = a.address
   AND s.city = a.city
   AND s.postalCode = a.postalCode
   AND s.country = a.country;

SELECT * FROM dim_suppliers;

CREATE OR REPLACE TABLE dim_employees AS
SELECT 
    id as employee_id,
    firstname || ' ' || lastname AS name
FROM employees_staging;

SELECT * FROM dim_employees;

CREATE OR REPLACE TABLE dim_products AS
SELECT 
    p.id AS product_id,
    p.productname,
    c.categoryName,
    c.description AS categoryDescription,
    p.unit
FROM products_staging p
JOIN categories_staging c ON p.categoryid = c.id;

SELECT * FROM dim_products;

CREATE OR REPLACE TABLE bridge_orders_products AS
SELECT 
    id,
    orderid,
    productid
FROM orderdetails_staging;

SELECT * FROM bridge_orders_products;

CREATE OR REPLACE TABLE dim_date AS
SELECT
    DISTINCT
    TO_DATE(TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS date,
    DATE_PART('year', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS year,
    DATE_PART('month', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS month,
    DATE_PART('day', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS day,
    CASE DATE_PART(dow, TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) 
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS dayOfWeekAsString
FROM orders_staging;

SELECT * FROM dim_date;

CREATE OR REPLACE TABLE fact_orders AS
SELECT
    o.id AS order_id,
    od.quantity AS product_quantity,
    ps.price AS product_price,
    (od.quantity * ps.price) AS total_price,
    b.id AS bridge_id,
    TO_DATE(TO_TIMESTAMP(o.orderDate, 'YYYY-MM-DD HH24:MI:SS')) AS date_id,
    e.employee_id,
    ps.supplierId AS supplier_id,
    c.customer_id AS customer_id
FROM orders_staging o
JOIN bridge_orders_products b ON o.id = b.orderid
JOIN orderdetails_staging od ON b.orderid = od.orderid AND b.productid = od.productid
JOIN products_staging ps ON b.productid = ps.id
JOIN dim_employees e ON o.employeeid = e.employee_id
JOIN dim_customers c ON o.customerid = c.customer_id;

SELECT * FROM fact_orders;


DROP IF EXISTS products_staging;
DROP IF EXISTS orders_staging;
DROP IF EXISTS customers_staging;
DROP IF EXISTS categories_staging;
DROP IF EXISTS employees_staging;
DROP IF EXISTS orderdetails_staging;
DROP IF EXISTS shippers_staging;
DROP IF EXISTS suppliers_staging;