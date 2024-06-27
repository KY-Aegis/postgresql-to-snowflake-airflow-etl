-- Create Customers_Table
CREATE TABLE IF NOT EXISTS Customers_Table (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    address JSONB,
    created_on TIMESTAMP,
    updated_on TIMESTAMP
);

-- Create Products_Table
CREATE TABLE IF NOT EXISTS Products_Table (
    product_id INT PRIMARY KEY,
    name VARCHAR(255),
    description VARCHAR(255),
    price FLOAT,
    created_on TIMESTAMP,
    updated_on TIMESTAMP
);

-- Create Orders_Table
CREATE TABLE IF NOT EXISTS Orders_Table (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    order_date DATE,
    created_on TIMESTAMP,
    updated_on TIMESTAMP
);

-- Create Sales_Data_Table
CREATE TABLE IF NOT EXISTS Sales_Data_Table (
    sale_id INT PRIMARY KEY,
    order_id INT,
    total_amount FLOAT,
    sale_date DATE,
    created_on TIMESTAMP,
    updated_on TIMESTAMP
);
