-- Create region table with identity key and validation constraints
CREATE TABLE region (
    region_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_name VARCHAR(50) NOT NULL UNIQUE
);

-- Add check constraint to ensure region_name is not empty
ALTER TABLE region 
ADD CONSTRAINT chk_region_name_not_empty 
CHECK (TRIM(region_name) != '');

-- Create country table with appropriate constraints
CREATE TABLE country (
    country_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);

-- Add check constraint to ensure country_name is not empty
ALTER TABLE country 
ADD CONSTRAINT chk_country_name_not_empty 
CHECK (TRIM(country_name) != '');

-- Create category table with validation constraints
CREATE TABLE category (
    category_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL UNIQUE
);

-- Add check constraint to ensure category_name is not empty
ALTER TABLE category 
ADD CONSTRAINT chk_category_name_not_empty 
CHECK (TRIM(category_name) != '');

-- Create sub_category table with proper foreign key relationships
CREATE TABLE sub_category (
    sub_category_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sub_category_name VARCHAR(50) NOT NULL,
    category_id BIGINT NOT NULL,
    CONSTRAINT fk_sub_category_category
        FOREIGN KEY (category_id)
        REFERENCES category(category_id)
        ON DELETE RESTRICT
);

-- Create unique constraint for subcategory name within a category
ALTER TABLE sub_category
ADD CONSTRAINT uq_sub_category_name_per_category
UNIQUE (sub_category_name, category_id);

-- Add check constraint to ensure sub_category_name is not empty
ALTER TABLE sub_category 
ADD CONSTRAINT chk_sub_category_name_not_empty 
CHECK (TRIM(sub_category_name) != '');

-- Create segment table for customer segmentation
CREATE TABLE segment (
    segment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    segment_name VARCHAR(50) NOT NULL UNIQUE
);

-- Add check constraint to ensure segment_name is not empty
ALTER TABLE segment 
ADD CONSTRAINT chk_segment_name_not_empty 
CHECK (TRIM(segment_name) != '');

-- Create state table with geographic hierarchy relationships
CREATE TABLE state (
    state_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state_name VARCHAR(50) NOT NULL,
    country_id BIGINT NOT NULL,
    region_id BIGINT NOT NULL,
    CONSTRAINT fk_state_country
        FOREIGN KEY (country_id)
        REFERENCES country(country_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_state_region
        FOREIGN KEY (region_id)
        REFERENCES region(region_id)
        ON DELETE RESTRICT
);

-- Create unique constraint for state names within a country
ALTER TABLE state
ADD CONSTRAINT uq_state_name_per_country
UNIQUE (state_name, country_id);

-- Create city table with appropriate constraints
CREATE TABLE city (
    city_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    state_id BIGINT NOT NULL,
    CONSTRAINT fk_city_state
        FOREIGN KEY (state_id)
        REFERENCES state(state_id)
        ON DELETE RESTRICT
);

-- Create unique constraint for city names within a state
ALTER TABLE city
ADD CONSTRAINT uq_city_name_per_state
UNIQUE (city_name, state_id);

-- Add check constraint to ensure city_name is not empty
ALTER TABLE city 
ADD CONSTRAINT chk_city_name_not_empty 
CHECK (TRIM(city_name) != '');

-- Create ship_mode table for shipping methods
CREATE TABLE ship_mode (
    ship_mode_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ship_mode_name VARCHAR(50) NOT NULL UNIQUE
);

-- Add check constraint to ensure ship_mode_name is not empty and from a predefined set
ALTER TABLE ship_mode 
ADD CONSTRAINT chk_ship_mode_name_valid 
CHECK (ship_mode_name IN ('Standard Class', 'Second Class', 'First Class', 'Same Day'));

-- Create sequence for customer_id generation
CREATE SEQUENCE customer_id_seq START WITH 1 INCREMENT BY 1;

-- Create customer table with appropriate constraints
CREATE TABLE customer (
    customer_id VARCHAR(50) PRIMARY KEY DEFAULT 'CUST-' || LPAD(NEXTVAL('customer_id_seq')::TEXT, 6, '0'),
    customer_name VARCHAR(100) NOT NULL,
    segment_id BIGINT NOT NULL,
    city_id BIGINT NOT NULL,
    CONSTRAINT fk_customer_segment
        FOREIGN KEY (segment_id)
        REFERENCES segment(segment_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_customer_city
        FOREIGN KEY (city_id)
        REFERENCES city(city_id)
        ON DELETE RESTRICT
);

-- Add check constraint to ensure customer_name is not empty
ALTER TABLE customer 
ADD CONSTRAINT chk_customer_name_not_empty 
CHECK (TRIM(customer_name) != '');

-- Create product table with appropriate constraints
CREATE TABLE product (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    sub_category_id BIGINT NOT NULL,
    CONSTRAINT fk_product_sub_category
        FOREIGN KEY (sub_category_id)
        REFERENCES sub_category(sub_category_id)
        ON DELETE RESTRICT
);

-- Add check constraint to ensure product_name is not empty
ALTER TABLE product 
ADD CONSTRAINT chk_product_name_not_empty 
CHECK (TRIM(product_name) != '');

-- Add check constraint to ensure product_id follows the pattern (e.g., FUR-BO-10001798)
ALTER TABLE product 
ADD CONSTRAINT chk_product_id_format 
CHECK (product_id ~ '^[A-Z]{3}-[A-Z]{2}-[0-9]{8}
);

-- Create sequence for order_id generation
CREATE SEQUENCE order_id_seq START WITH 1 INCREMENT BY 1;

-- Create order table with appropriate constraints
CREATE TABLE "order" (
    order_id VARCHAR(50) PRIMARY KEY DEFAULT 'ORD-' || LPAD(NEXTVAL('order_id_seq')::TEXT, 6, '0'),
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    ship_date DATE,
    ship_mode_id BIGINT NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    CONSTRAINT fk_order_ship_mode
        FOREIGN KEY (ship_mode_id)
        REFERENCES ship_mode(ship_mode_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_order_customer
        FOREIGN KEY (customer_id)
        REFERENCES customer(customer_id)
        ON DELETE RESTRICT
);

-- Add check constraint to ensure ship_date is after or equal to order_date
ALTER TABLE "order" 
ADD CONSTRAINT chk_ship_date_after_order_date 
CHECK (ship_date IS NULL OR ship_date >= order_date);

-- Create order_product table (junction table with additional attributes)
CREATE TABLE order_product (
    order_product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity BIGINT NOT NULL,
    sales DECIMAL(10, 4) NOT NULL,
    profit DECIMAL(10, 4) NOT NULL,
    CONSTRAINT fk_order_product_order
        FOREIGN KEY (order_id)
        REFERENCES "order"(order_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_order_product_product
        FOREIGN KEY (product_id)
        REFERENCES product(product_id)
        ON DELETE RESTRICT
);

-- Create unique constraint to prevent duplicate product entries in the same order
ALTER TABLE order_product
ADD CONSTRAINT uq_order_product
UNIQUE (order_id, product_id);

-- Add check constraint to ensure quantity is positive
ALTER TABLE order_product 
ADD CONSTRAINT chk_quantity_positive 
CHECK (quantity > 0);

-- Add check constraint to ensure sales is not negative
ALTER TABLE order_product 
ADD CONSTRAINT chk_sales_not_negative 
CHECK (sales >= 0);

-- Create STORED derived column for profit margin as a percentage
ALTER TABLE order_product
ADD COLUMN profit_margin DECIMAL(5, 2) GENERATED ALWAYS AS (
    CASE 
        WHEN sales > 0 THEN ROUND((profit / sales * 100)::NUMERIC, 2)
        ELSE 0
    END
) STORED;

-- Create index for common join conditions to improve query performance
CREATE INDEX idx_product_sub_category ON product(sub_category_id);
CREATE INDEX idx_customer_segment ON customer(segment_id);
CREATE INDEX idx_customer_city ON customer(city_id);
CREATE INDEX idx_city_state ON city(state_id);
CREATE INDEX idx_state_region ON state(region_id);
CREATE INDEX idx_order_customer ON "order"(customer_id);
CREATE INDEX idx_order_product_order ON order_product(order_id);
CREATE INDEX idx_order_product_product ON order_product(product_id);