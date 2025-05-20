CREATE TEMPORARY TABLE temp_superstore_data (
    region VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(255),
    sales DECIMAL(10, 4),
    quantity INTEGER,
    profit DECIMAL(10, 4)
);

-- Function to validate and clean data before insertion
CREATE OR REPLACE FUNCTION clean_superstore_record(
    p_region VARCHAR, 
    p_product_id VARCHAR, 
    p_category VARCHAR, 
    p_sub_category VARCHAR, 
    p_product_name VARCHAR, 
    p_sales DECIMAL, 
    p_quantity INTEGER, 
    p_profit DECIMAL
) RETURNS BOOLEAN AS $
BEGIN
    -- Check for missing critical data
    IF p_region IS NULL OR TRIM(p_region) = '' OR
       p_product_id IS NULL OR TRIM(p_product_id) = '' OR
       p_category IS NULL OR TRIM(p_category) = '' OR
       p_sub_category IS NULL OR TRIM(p_sub_category) = '' OR
       p_product_name IS NULL OR TRIM(p_product_name) = '' THEN
        RETURN FALSE;
    END IF;
    
    -- Check for valid numeric data
    IF p_quantity <= 0 OR p_sales < 0 THEN
        RETURN FALSE;
    END IF;
    
    -- Additional validation can be added here
    
    RETURN TRUE;
END;
$ LANGUAGE plpgsql;

-- Transaction for data loading to ensure atomicity
BEGIN;

-- Sample data insertion with validation
-- In production, this would be a COPY command from a CSV file
DO $
DECLARE
    valid_records INTEGER := 0;
    invalid_records INTEGER := 0;
    is_valid BOOLEAN;
BEGIN
    -- Insert sample data with validation
    SELECT clean_superstore_record('South', 'FUR-BO-10001798', 'Furniture', 'Bookcases', 'Bush Somerset Collection Bookcase', 261.96, 2, 41.9136) INTO is_valid;
    IF is_valid THEN
        INSERT INTO temp_superstore_data VALUES ('South', 'FUR-BO-10001798', 'Furniture', 'Bookcases', 'Bush Somerset Collection Bookcase', 261.96, 2, 41.9136);
        valid_records := valid_records + 1;
    ELSE
        invalid_records := invalid_records + 1;
    END IF;
    
    SELECT clean_superstore_record('South', 'FUR-CH-10000454', 'Furniture', 'Chairs', 'Hon Deluxe Fabric Upholstered Stacking Chairs, Rounded Back', 731.94, 3, 219.582) INTO is_valid;
    IF is_valid THEN
        INSERT INTO temp_superstore_data VALUES ('South', 'FUR-CH-10000454', 'Furniture', 'Chairs', 'Hon Deluxe Fabric Upholstered Stacking Chairs, Rounded Back', 731.94, 3, 219.582);
        valid_records := valid_records + 1;
    ELSE
        invalid_records := invalid_records + 1;
    END IF;
    
    SELECT clean_superstore_record('West', 'OFF-LA-10000240', 'Office Supplies', 'Labels', 'Self-Adhesive Address Labels for Typewriters by Universal', 14.62, 2, 6.8714) INTO is_valid;
    IF is_valid THEN
        INSERT INTO temp_superstore_data VALUES ('West', 'OFF-LA-10000240', 'Office Supplies', 'Labels', 'Self-Adhesive Address Labels for Typewriters by Universal', 14.62, 2, 6.8714);
        valid_records := valid_records + 1;
    ELSE
        invalid_records := invalid_records + 1;
    END IF;
    
    SELECT clean_superstore_record('South', 'OFF-PA-10002365', 'Office Supplies', 'Paper', 'Xerox 1967', 15.552, 3, 5.4432) INTO is_valid;
    IF is_valid THEN
        INSERT INTO temp_superstore_data VALUES ('South', 'OFF-PA-10002365', 'Office Supplies', 'Paper', 'Xerox 1967', 15.552, 3, 5.4432);
        valid_records := valid_records + 1;
    ELSE
        invalid_records := invalid_records + 1;
    END IF;
    
    SELECT clean_superstore_record('West', 'OFF-BI-10003656', 'Office Supplies', 'Binders', 'Fellowes PB200 Plastic Comb Binding Machine', 407.976, 3, 132.5922) INTO is_valid;
    IF is_valid THEN
        INSERT INTO temp_superstore_data VALUES ('West', 'OFF-BI-10003656', 'Office Supplies', 'Binders', 'Fellowes PB200 Plastic Comb Binding Machine', 407.976, 3, 132.5922);
        valid_records := valid_records + 1;
    ELSE
        invalid_records := invalid_records + 1;
    END IF;
    
    -- Log results
    RAISE NOTICE 'Data import summary: % valid records imported, % invalid records skipped', valid_records, invalid_records;
END $;

-- Populate dimension tables first using a systematic approach

-- 1. Region table population
INSERT INTO region (region_name)
SELECT DISTINCT region 
FROM temp_superstore_data
WHERE region IS NOT NULL AND TRIM(region) != ''
ON CONFLICT (region_name) DO NOTHING;

-- 2. Country table population (assuming US as default country)
INSERT INTO country (country_name) 
VALUES ('United States')
ON CONFLICT (country_name) DO NOTHING;

-- 3. Category table population
INSERT INTO category (category_name)
SELECT DISTINCT category
FROM temp_superstore_data
WHERE category IS NOT NULL AND TRIM(category) != ''
ON CONFLICT (category_name) DO NOTHING;

-- 4. Sub-category table population with proper category relationships
INSERT INTO sub_category (sub_category_name, category_id)
SELECT DISTINCT t.sub_category, c.category_id
FROM temp_superstore_data t
JOIN category c ON t.category = c.category_name
WHERE t.sub_category IS NOT NULL AND TRIM(t.sub_category) != ''
ON CONFLICT (sub_category_name, category_id) DO NOTHING;

-- 5. Segment table population (standard retail segments)
INSERT INTO segment (segment_name) 
VALUES ('Consumer'), ('Corporate'), ('Home Office')
ON CONFLICT (segment_name) DO NOTHING;

-- 6. State table population (using regions from dataset as proxy)
INSERT INTO state (state_name, country_id, region_id)
SELECT 
    DISTINCT t.region || ' State', 
    (SELECT country_id FROM country WHERE country_name = 'United States'), 
    r.region_id
FROM temp_superstore_data t
JOIN region r ON t.region = r.region_name
ON CONFLICT (state_name, country_id) DO NOTHING;

-- 7. City table population (creating representative cities for each state)
INSERT INTO city (city_name, state_id)
SELECT 
    s.state_name || ' City', 
    s.state_id
FROM state s
ON CONFLICT (city_name, state_id) DO NOTHING;

-- 8. Ship mode table population
INSERT INTO ship_mode (ship_mode_name) 
VALUES 
    ('Standard Class'),
    ('Second Class'),
    ('First Class'),
    ('Same Day')
ON CONFLICT (ship_mode_name) DO NOTHING;

-- 9. Customer table population - create representative customers for each segment/city combination
INSERT INTO customer (customer_name, segment_id, city_id)
SELECT 
    'Customer from ' || c.city_name || ' - ' || s.segment_name, 
    s.segment_id, 
    c.city_id
FROM segment s
CROSS JOIN city c
LIMIT 20; -- Limiting to a reasonable number of customers

-- 10. Product table population - preserving actual product data from the dataset
INSERT INTO product (product_id, product_name, sub_category_id)
SELECT DISTINCT 
    t.product_id, 
    t.product_name, 
    sc.sub_category_id
FROM temp_superstore_data t
JOIN sub_category sc ON t.sub_category = sc.sub_category_name
JOIN category c ON t.category = c.category_name AND sc.category_id = c.category_id
WHERE t.product_id IS NOT NULL AND t.product_id != ''
ON CONFLICT (product_id) DO NOTHING;

-- 11. Order table population - create realistic orders
-- Create a series of dates for the last year
WITH date_series AS (
    SELECT 
        generate_series(
            CURRENT_DATE - INTERVAL '1 year', 
            CURRENT_DATE, 
            '1 day'
        )::DATE AS order_date
)
INSERT INTO "order" (order_id, order_date, ship_date, ship_mode_id, customer_id)
SELECT 
    'ORD-' || LPAD(ROW_NUMBER() OVER (ORDER BY d.order_date, c.customer_id)::TEXT, 6, '0') AS order_id,
    d.order_date,
    d.order_date + (FLOOR(RANDOM() * 5) + 1)::INTEGER AS ship_date, -- Ship 1-5 days later
    (SELECT ship_mode_id FROM ship_mode ORDER BY RANDOM() LIMIT 1),
    c.customer_id
FROM date_series d
CROSS JOIN (SELECT customer_id FROM customer ORDER BY customer_id LIMIT 5) c
WHERE EXTRACT(DOW FROM d.order_date) BETWEEN 1 AND 5 -- Only create orders on weekdays
AND RANDOM() < 0.2 -- Only create orders for about 20% of the days to avoid too many records
LIMIT 50; -- Limit total number of orders

-- 12. Order-Product table population - connect products to orders with actual sales data
INSERT INTO order_product (order_id, product_id, quantity, sales, profit)
SELECT 
    o.order_id,
    p.product_id,
    GREATEST(1, FLOOR(RANDOM() * 5)::INTEGER), -- Quantity between 1 and 5
    ROUND((RANDOM() * 1000)::NUMERIC, 2), -- Random sales amount
    ROUND((RANDOM() * 200 - 50)::NUMERIC, 2) -- Random profit (can be negative)
FROM 
    "order" o
CROSS JOIN (SELECT product_id FROM product ORDER BY RANDOM() LIMIT 3) p -- Each order has up to 3 products
ORDER BY o.order_id, p.product_id
ON CONFLICT (order_id, product_id) DO NOTHING;

-- Update the order_product table with actual data from the temporary table where possible
WITH source_data AS (
    SELECT 
        MIN(o.order_id) AS order_id,
        t.product_id,
        t.quantity,
        t.sales,
        t.profit
    FROM temp_superstore_data t
    JOIN product p ON t.product_id = p.product_id
    CROSS JOIN (SELECT order_id FROM "order" ORDER BY order_id LIMIT 1) o
    GROUP BY t.product_id, t.quantity, t.sales, t.profit
)
UPDATE order_product op
SET 
    quantity = sd.quantity,
    sales = sd.sales,
    profit = sd.profit
FROM source_data sd
WHERE op.product_id = sd.product_id
  AND op.order_id = sd.order_id;

-- Verify data integrity between tables
DO $
DECLARE
    issue_count INTEGER := 0;
BEGIN
    -- Check for orphaned product records
    SELECT COUNT(*) INTO issue_count
    FROM product p
    LEFT JOIN sub_category sc ON p.sub_category_id = sc.sub_category_id
    WHERE sc.sub_category_id IS NULL;
    
    IF issue_count > 0 THEN
        RAISE NOTICE 'Found % orphaned product records', issue_count;
    END IF;
    
    -- Check for orphaned order_product records
    SELECT COUNT(*) INTO issue_count
    FROM order_product op
    LEFT JOIN "order" o ON op.order_id = o.order_id
    WHERE o.order_id IS NULL;
    
    IF issue_count > 0 THEN
        RAISE NOTICE 'Found % orphaned order_product records', issue_count;
    END IF;
    
    -- Check for invalid quantity values
    SELECT COUNT(*) INTO issue_count
    FROM order_product
    WHERE quantity <= 0;
    
    IF issue_count > 0 THEN
        RAISE NOTICE 'Found % order_product records with invalid quantities', issue_count;
    END IF;
    
    -- Check for non-unique product IDs
    SELECT COUNT(*) - COUNT(DISTINCT product_id) INTO issue_count
    FROM product;
    
    IF issue_count > 0 THEN
        RAISE NOTICE 'Found % duplicate product IDs', issue_count;
    END IF;
END $;

-- Clean up temporary objects
DROP FUNCTION IF EXISTS clean_superstore_record;
DROP TABLE temp_superstore_data;

-- Commit the transaction
COMMIT;Corporate'), ('Home Office');

-- Insert data into state table (using region as proxy since states aren't in sample data)
INSERT INTO state (state_name, country_id, region_id)
SELECT DISTINCT t.region, 1, r.region_id
FROM temp_superstore_data t
JOIN region r ON t.region = r.region_name;

-- Insert data into city table (for this exercise, we'll create default cities)
INSERT INTO city (city_name, state_id)
SELECT 'Default City ' || s.state_id, s.state_id
FROM state s;

-- Insert data into ship_mode table
INSERT INTO ship_mode (ship_mode_name) VALUES ('Standard Class'), ('Second Class'), ('First Class'), ('Same Day');

-- Insert data into customer table (for this exercise, we'll create default customers)
INSERT INTO customer (customer_name, segment_id, city_id)
SELECT 'Customer ' || s.segment_id || '-' || c.city_id, s.segment_id, c.city_id
FROM segment s
CROSS JOIN city c;

-- Insert data into product table
INSERT INTO product (product_id, product_name, sub_category_id)
SELECT DISTINCT t.product_id, t.product_name, sc.sub_category_id
FROM temp_superstore_data t
JOIN sub_category sc ON t.sub_category = sc.sub_category_name
WHERE t.product_id IS NOT NULL AND t.product_id != '';

-- Insert data into order table (create one order per product for this exercise)
INSERT INTO "order" (order_id, order_date, ship_date, ship_mode_id, customer_id)
SELECT 
    'ORD-' || ROW_NUMBER() OVER () AS order_id,
    CURRENT_DATE - (RANDOM() * 365)::INTEGER AS order_date,
    CURRENT_DATE - (RANDOM() * 180)::INTEGER AS ship_date,
    (RANDOM() * 3 + 1)::INTEGER AS ship_mode_id,
    (RANDOM() * (SELECT COUNT(*) FROM customer))::INTEGER + 1 AS customer_id
FROM temp_superstore_data;

-- Insert data into order_product table
INSERT INTO order_product (order_id, product_id, quantity, sales, profit)
SELECT 
    o.order_id, 
    t.product_id, 
    t.quantity, 
    t.sales, 
    t.profit
FROM temp_superstore_data t
JOIN "order" o ON o.order_id = 'ORD-' || ROW_NUMBER() OVER (ORDER BY t.product_id);

-- Drop temporary table
DROP TABLE temp_superstore_data;