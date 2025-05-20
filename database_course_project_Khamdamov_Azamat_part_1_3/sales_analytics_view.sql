-- This view presents analytics data from the database, excluding surrogate keys and duplicate entries
-- It joins multiple tables to provide comprehensive sales insights by region, category, and customer segment

CREATE OR REPLACE VIEW sales_analytics_view AS
SELECT 
    -- Regional Information
    r.region_name,
    c.country_name,
    s.state_name,
    ci.city_name,
    
    -- Customer Information
    cust.customer_name,
    seg.segment_name,
    
    -- Product Information
    cat.category_name,
    subcat.sub_category_name,
    p.product_name,
    
    -- Order Information
    o.order_date,
    o.ship_date,
    sm.ship_mode_name,
    
    -- Sales Metrics
    op.quantity,
    op.sales,
    op.profit,
    ROUND((op.profit / NULLIF(op.sales, 0)) * 100, 2) AS profit_margin_percent,
    CASE 
        WHEN op.profit < 0 THEN 'Loss'
        WHEN op.profit = 0 THEN 'Break-even'
        ELSE 'Profit'
    END AS profit_status,
    -- PostgreSQL specific date difference calculation
    (o.ship_date - o.order_date) AS days_to_ship
FROM 
    order_product op
    JOIN product p ON op.product_id = p.product_id
    JOIN sub_category subcat ON p.sub_category_id = subcat.sub_category_id
    JOIN category cat ON subcat.category_id = cat.category_id
    JOIN "order" o ON op.order_id = o.order_id
    JOIN customer cust ON o.customer_id = cust.customer_id
    JOIN segment seg ON cust.segment_id = seg.segment_id
    JOIN city ci ON cust.city_id = ci.city_id
    JOIN state s ON ci.state_id = s.state_id
    JOIN country c ON s.country_id = c.country_id
    JOIN region r ON s.region_id = r.region_id
    JOIN ship_mode sm ON o.ship_mode_id = sm.ship_mode_id
WHERE 
    -- Ensure we only get valid sales data
    op.sales > 0 
ORDER BY 
    o.order_date DESC,
    region_name,
    category_name;

-- Example queries using the view (PostgreSQL specific):
-- 1. Get overall sales and profit by region
-- SELECT region_name, SUM(sales)::NUMERIC(10,2) as total_sales, SUM(profit)::NUMERIC(10,2) as total_profit 
-- FROM sales_analytics_view 
-- GROUP BY region_name
-- ORDER BY total_sales DESC;

-- 2. Find top performing product categories by profit margin
-- SELECT category_name, sub_category_name, 
--       SUM(sales)::NUMERIC(10,2) as total_sales, 
--       SUM(profit)::NUMERIC(10,2) as total_profit,
--       ROUND((SUM(profit) / NULLIF(SUM(sales), 0) * 100)::NUMERIC, 2) as profit_margin
-- FROM sales_analytics_view
-- GROUP BY category_name, sub_category_name
-- ORDER BY profit_margin DESC;
