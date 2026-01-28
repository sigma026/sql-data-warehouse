/*
Витрина 1: Customer Analytics Dashboard
Анализ клиентов по демографии, активности и тратам
*/
DROP TABLE IF EXISTS analytics.customer_dashboard;
CREATE TABLE analytics.customer_dashboard AS
SELECT 
    -- Основная информация о клиенте
    c.cst_id,
    c.cst_key,
    c.cst_firstname || ' ' || c.cst_lastname AS full_name,
    c.cst_gndr AS gender,
    c.cst_marital_status AS marital_status,
    c.cst_create_date AS registration_date,
    
    -- Демографические данные из ERP
    e.bdate AS birth_date,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.bdate)) AS age,
    CASE 
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.bdate)) < 25 THEN '18-24'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.bdate)) < 35 THEN '25-34'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.bdate)) < 45 THEN '35-44'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.bdate)) < 55 THEN '45-54'
        ELSE '55+'
    END AS age_group,
    
    -- Статистика покупок
    COUNT(DISTINCT s.sls_ord_num) AS total_orders,
    COUNT(s.sls_prd_key) AS total_items_purchased,
    SUM(s.sls_quantity) AS total_quantity,
    SUM(s.sls_sales) AS total_spent,
    ROUND(AVG(s.sls_sales), 2) AS avg_order_value,
    MAX(s.sls_order_dt) AS last_order_date,
    
    -- Сегментация по тратам
    CASE 
        WHEN SUM(s.sls_sales) > 100000 THEN 'VIP'
        WHEN SUM(s.sls_sales) > 50000 THEN 'Premium'
        WHEN SUM(s.sls_sales) > 20000 THEN 'Regular'
        ELSE 'Standard'
    END AS customer_segment,
    
    -- Предпочтения по категориям
    MODE() WITHIN GROUP (ORDER BY p.prd_line) AS favorite_category,
    STRING_AGG(DISTINCT p.prd_line, ', ') AS all_categories
    
FROM crm_cust_info c
LEFT JOIN erp_cust_az12 e ON c.cst_key = e.cid
LEFT JOIN crm_sales_details s ON c.cst_id = s.sls_cust_id
LEFT JOIN crm_prd_info p ON s.sls_prd_key = p.prd_key
GROUP BY 
    c.cst_id, c.cst_key, c.cst_firstname, c.cst_lastname, 
    c.cst_gndr, c.cst_marital_status, c.cst_create_date,
    e.bdate
ORDER BY total_spent DESC;



/*
Витрина 2: Product Performance Dashboard
Анализ продуктов по продажам, рентабельности и категориям
*/
DROP TABLE IF EXISTS analytics.product_performance;
CREATE TABLE analytics.product_performance AS
SELECT 
    -- Информация о продукте
    p.prd_id,
    p.prd_key,
    p.prd_nm AS product_name,
    p.prd_line AS product_line,
    p.prd_cost AS cost_price,
    
    -- Категорийная информация из ERP
    cat.cat AS product_category,
    cat.subcat AS product_subcategory,
    cat.maintenance AS category_maintenance,
    
    -- Статистика продаж
    COUNT(DISTINCT s.sls_ord_num) AS total_orders,
    SUM(s.sls_quantity) AS total_quantity_sold,
    SUM(s.sls_sales) AS total_revenue,
    ROUND(AVG(s.sls_price), 2) AS avg_selling_price,
    MIN(s.sls_price) AS min_price,
    MAX(s.sls_price) AS max_price,
    
    -- Финансовые метрики
    ROUND((AVG(s.sls_price) - p.prd_cost)::numeric, 2) AS avg_profit_per_unit,
    ROUND(((AVG(s.sls_price) - p.prd_cost) / p.prd_cost * 100)::numeric, 2) AS margin_percentage,
    ROUND(SUM(s.sls_sales) - (p.prd_cost * SUM(s.sls_quantity)), 2) AS total_profit,
    
    -- Активность продукта
    p.prd_start_dt AS product_start_date,
    p.prd_end_dt AS product_end_date,
    CASE 
        WHEN p.prd_end_dt < CURRENT_DATE THEN 'Discontinued'
        WHEN p.prd_start_dt > CURRENT_DATE THEN 'Not Started'
        ELSE 'Active'
    END AS product_status,
    
    -- Распределение по клиентам
    COUNT(DISTINCT s.sls_cust_id) AS unique_customers,
    STRING_AGG(DISTINCT 
        CASE 
            WHEN s.sls_quantity > 5 THEN 'Bulk Buyer'
            WHEN s.sls_quantity > 2 THEN 'Regular Buyer'
            ELSE 'Single Buyer'
        END, ', '
    ) AS customer_segments,
    
    -- Топ клиенты продукта
    (SELECT c.cst_firstname || ' ' || c.cst_lastname 
     FROM crm_sales_details s2
     JOIN crm_cust_info c ON s2.sls_cust_id = c.cst_id
     WHERE s2.sls_prd_key = p.prd_key
     GROUP BY c.cst_id, c.cst_firstname, c.cst_lastname
     ORDER BY SUM(s2.sls_quantity) DESC
     LIMIT 1) AS top_customer
    
FROM crm_prd_info p
LEFT JOIN crm_sales_details s ON p.prd_key = s.sls_prd_key
LEFT JOIN erp_px_cat_g1v2 cat ON p.prd_line = cat.cat
GROUP BY 
    p.prd_id, p.prd_key, p.prd_nm, p.prd_line, p.prd_cost,
    p.prd_start_dt, p.prd_end_dt,
    cat.cat, cat.subcat, cat.maintenance
ORDER BY total_revenue DESC;




/*
Витрина 3: Sales Funnel & Time Analysis
Анализ воронки продаж, сезонности и временных трендов
*/
DROP TABLE IF EXISTS analytics.sales_time_analysis;
CREATE TABLE analytics.sales_time_analysis AS
WITH sales_with_dates AS (
    SELECT 
        s.*,
        -- Конвертация INT дат в DATE (предполагаем формат YYYYMMDD)
        TO_DATE(s.sls_order_dt::text, 'YYYYMMDD') AS order_date,
        TO_DATE(s.sls_ship_dt::text, 'YYYYMMDD') AS ship_date,
        TO_DATE(s.sls_due_dt::text, 'YYYYMMDD') AS due_date,
        
        -- Извлечение временных компонентов
        EXTRACT(YEAR FROM TO_DATE(s.sls_order_dt::text, 'YYYYMMDD')) AS order_year,
        EXTRACT(MONTH FROM TO_DATE(s.sls_order_dt::text, 'YYYYMMDD')) AS order_month,
        EXTRACT(QUARTER FROM TO_DATE(s.sls_order_dt::text, 'YYYYMMDD')) AS order_quarter,
        EXTRACT(DOW FROM TO_DATE(s.sls_order_dt::text, 'YYYYMMDD')) AS order_day_of_week,
        
        -- Метрики выполнения заказов
        TO_DATE(s.sls_ship_dt::text, 'YYYYMMDD') - TO_DATE(s.sls_order_dt::text, 'YYYYMMDD') AS days_to_ship,
        TO_DATE(s.sls_due_dt::text, 'YYYYMMDD') - TO_DATE(s.sls_ship_dt::text, 'YYYYMMDD') AS days_until_due
        
    FROM crm_sales_details s
)
SELECT 
    -- Временные срезы
    d.order_year,
    d.order_month,
    TO_CHAR(TO_DATE(d.order_month::text, 'MM'), 'Month') AS month_name,
    d.order_quarter,
    d.order_day_of_week,
    CASE d.order_day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    
    -- Продуктовые категории
    p.prd_line AS product_line,
    cat.cat AS product_category,
    cat.subcat AS product_subcategory,
    
    -- Агрегированные метрики продаж
    COUNT(DISTINCT d.sls_ord_num) AS total_orders,
    COUNT(DISTINCT d.sls_cust_id) AS unique_customers,
    SUM(d.sls_quantity) AS total_quantity,
    SUM(d.sls_sales) AS total_revenue,
    ROUND(AVG(d.sls_price), 2) AS avg_price,
    
    -- Метрики выполнения заказов
    ROUND(AVG(d.days_to_ship), 1) AS avg_days_to_ship,
    ROUND(AVG(d.days_until_due), 1) AS avg_days_until_due,
    COUNT(CASE WHEN d.days_to_ship <= 2 THEN 1 END) AS fast_shipped_orders,
    COUNT(CASE WHEN d.days_to_ship > 5 THEN 1 END) AS slow_shipped_orders,
    
    -- Сезонные тренды
    CASE 
        WHEN d.order_month IN (12, 1, 2) THEN 'Winter'
        WHEN d.order_month IN (3, 4, 5) THEN 'Spring'
        WHEN d.order_month IN (6, 7, 8) THEN 'Summer'
        WHEN d.order_month IN (9, 10, 11) THEN 'Autumn'
    END AS season,
    
    -- Демография покупателей
    MODE() WITHIN GROUP (ORDER BY cust.cst_gndr) AS most_common_gender,
    MODE() WITHIN GROUP (ORDER BY cust.cst_marital_status) AS most_common_marital_status,
    
    -- Временные тренды
    LAG(SUM(d.sls_sales), 1) OVER (
        PARTITION BY p.prd_line, d.order_month 
        ORDER BY d.order_year
    ) AS prev_year_month_revenue,
    
    ROUND(
        ((SUM(d.sls_sales) - LAG(SUM(d.sls_sales), 1) OVER (
            PARTITION BY p.prd_line, d.order_month 
            ORDER BY d.order_year
        )) / LAG(SUM(d.sls_sales), 1) OVER (
            PARTITION BY p.prd_line, d.order_month 
            ORDER BY d.order_year
        ) * 100)::numeric, 2
    ) AS yoy_growth_percentage
    
FROM sales_with_dates d
JOIN crm_prd_info p ON d.sls_prd_key = p.prd_key
LEFT JOIN erp_px_cat_g1v2 cat ON p.prd_line = cat.cat
LEFT JOIN crm_cust_info cust ON d.sls_cust_id = cust.cst_id
GROUP BY 
    d.order_year, d.order_month, d.order_quarter, d.order_day_of_week,
    p.prd_line, cat.cat, cat.subcat
ORDER BY 
    d.order_year DESC, 
    d.order_month DESC, 
    total_revenue DESC;



---------------------------------




-- Материализованное представление ежедневных продаж
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_daily_sales;
CREATE MATERIALIZED VIEW analytics.mv_daily_sales AS
SELECT 
    TO_DATE(sls_order_dt::text, 'YYYYMMDD') AS sales_date,
    COUNT(DISTINCT sls_ord_num) AS orders_count,
    COUNT(*) AS items_sold,
    SUM(sls_quantity) AS total_quantity,
    SUM(sls_sales) AS total_revenue,
    ROUND(AVG(sls_price), 2) AS avg_price_per_item
FROM bronze.crm_sales_details
GROUP BY TO_DATE(sls_order_dt::text, 'YYYYMMDD')
ORDER BY sales_date DESC;

-- Индекс для быстрого поиска по дате
CREATE UNIQUE INDEX idx_mv_daily_sales_date 
ON analytics.mv_daily_sales(sales_date);












----------------------------------------




-- Материализованное представление топ продуктов
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_top_products;
CREATE MATERIALIZED VIEW analytics.mv_top_products AS
SELECT 
    p.prd_key,
    p.prd_nm AS product_name,
    p.prd_line AS category,
    COUNT(DISTINCT s.sls_ord_num) AS total_orders,
    SUM(s.sls_quantity) AS total_quantity_sold,
    SUM(s.sls_sales) AS total_revenue,
    ROUND(SUM(s.sls_sales) - (p.prd_cost * SUM(s.sls_quantity)), 2) AS total_profit,
    ROUND(((AVG(s.sls_price) - p.prd_cost) / p.prd_cost * 100), 2) AS margin_percent
FROM bronze.crm_prd_info p
JOIN bronze.crm_sales_details s ON p.prd_key = s.sls_prd_key
GROUP BY p.prd_key, p.prd_nm, p.prd_line, p.prd_cost
ORDER BY total_revenue DESC
LIMIT 10;

-- Индекс для быстрой сортировки
CREATE UNIQUE INDEX idx_mv_top_products_revenue 
ON analytics.mv_top_products(prd_key);










---------------------------------------------


-- Материализованное представление клиентских сегментов
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_customer_segments;
CREATE MATERIALIZED VIEW analytics.mv_customer_segments AS
SELECT 
    c.cst_id,
    c.cst_key,
    c.cst_firstname || ' ' || c.cst_lastname AS customer_name,
    c.cst_gndr AS gender,
    CASE 
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.bdate)) < 30 THEN '18-29'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.bdate)) < 40 THEN '30-39'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.bdate)) < 50 THEN '40-49'
        ELSE '50+'
    END AS age_group,
    COUNT(DISTINCT s.sls_ord_num) AS total_orders,
    SUM(s.sls_sales) AS total_spent,
    CASE 
        WHEN SUM(s.sls_sales) >= 100000 THEN 'VIP'
        WHEN SUM(s.sls_sales) >= 50000 THEN 'Premium'
        WHEN SUM(s.sls_sales) >= 20000 THEN 'Regular'
        WHEN SUM(s.sls_sales) >= 5000 THEN 'Standard'
        ELSE 'New/Low'
    END AS spending_segment,
    MAX(TO_DATE(s.sls_order_dt::text, 'YYYYMMDD')) AS last_purchase_date
FROM bronze.crm_cust_info c
LEFT JOIN bronze.erp_cust_az12 e ON c.cst_key = e.cid
LEFT JOIN bronze.crm_sales_details s ON c.cst_id = s.sls_cust_id
GROUP BY c.cst_id, c.cst_key, c.cst_firstname, c.cst_lastname, 
         c.cst_gndr, e.bdate
ORDER BY total_spent DESC;

-- Индекс по сегментам
CREATE INDEX idx_mv_customer_segments_segment 
ON analytics.mv_customer_segments(spending_segment, total_spent);




-----------------------------



-- Материализованное представление по категориям
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_category_sales;
CREATE MATERIALIZED VIEW analytics.mv_category_sales AS
SELECT 
    p.prd_line AS category,
    cat.cat AS erp_category,
    cat.subcat AS subcategory,
    COUNT(DISTINCT p.prd_id) AS products_count,
    COUNT(DISTINCT s.sls_ord_num) AS orders_count,
    SUM(s.sls_quantity) AS total_quantity_sold,
    SUM(s.sls_sales) AS total_revenue,
    ROUND(AVG(s.sls_price), 2) AS avg_price,
    ROUND(SUM(s.sls_sales) / NULLIF(COUNT(DISTINCT s.sls_ord_num), 0), 2) AS avg_order_value
FROM bronze.crm_prd_info p
LEFT JOIN bronze.crm_sales_details s ON p.prd_key = s.sls_prd_key
LEFT JOIN bronze.erp_px_cat_g1v2 cat ON p.prd_line = cat.cat
GROUP BY p.prd_line, cat.cat, cat.subcat
ORDER BY total_revenue DESC;

-- Индекс по категориям
CREATE UNIQUE INDEX idx_mv_category_sales_cat 
ON analytics.mv_category_sales(category);









------------------------


-- Материализованное представление месячных KPI
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_monthly_kpi;
CREATE MATERIALIZED VIEW analytics.mv_monthly_kpi AS
SELECT 
    EXTRACT(YEAR FROM TO_DATE(sls_order_dt::text, 'YYYYMMDD')) AS year,
    EXTRACT(MONTH FROM TO_DATE(sls_order_dt::text, 'YYYYMMDD')) AS month,
    TO_CHAR(TO_DATE(sls_order_dt::text, 'YYYYMMDD'), 'Month') AS month_name,
    
    -- Основные KPI
    COUNT(DISTINCT sls_ord_num) AS total_orders,
    COUNT(DISTINCT sls_cust_id) AS unique_customers,
    SUM(sls_quantity) AS total_items_sold,
    SUM(sls_sales) AS total_revenue,
    
    -- Средние значения
    ROUND(AVG(sls_price), 2) AS avg_item_price,
    ROUND(SUM(sls_sales) / NULLIF(COUNT(DISTINCT sls_ord_num), 0), 2) AS avg_order_value,
    
    -- Конверсия (если бы была воронка)
    COUNT(DISTINCT sls_cust_id) AS new_customers_this_month,
    
    -- Сравнение с прошлым месяцем
    LAG(COUNT(DISTINCT sls_ord_num), 1) OVER (ORDER BY 
        EXTRACT(YEAR FROM TO_DATE(sls_order_dt::text, 'YYYYMMDD')),
        EXTRACT(MONTH FROM TO_DATE(sls_order_dt::text, 'YYYYMMDD'))
    ) AS prev_month_orders,
    
    LAG(SUM(sls_sales), 1) OVER (ORDER BY 
        EXTRACT(YEAR FROM TO_DATE(sls_order_dt::text, 'YYYYMMDD')),
        EXTRACT(MONTH FROM TO_DATE(sls_order_dt::text, 'YYYYMMDD'))
    ) AS prev_month_revenue
    
FROM bronze.crm_sales_details
GROUP BY 
    EXTRACT(YEAR FROM TO_DATE(sls_order_dt::text, 'YYYYMMDD')),
    EXTRACT(MONTH FROM TO_DATE(sls_order_dt::text, 'YYYYMMDD')),
    TO_CHAR(TO_DATE(sls_order_dt::text, 'YYYYMMDD'), 'Month')
ORDER BY year DESC, month DESC;

-- Индекс по дате
CREATE UNIQUE INDEX idx_mv_monthly_kpi_date 
ON analytics.mv_monthly_kpi(year, month);








------------------



-- Ручное обновление (по расписанию в cron)
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_daily_sales;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_top_products;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_customer_segments;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_category_sales;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_monthly_kpi;

-- Или автоматически по триггеру (более сложно)











