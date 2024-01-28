WITH current_storage_cost AS (
    SELECT 
        currency, 
        effective_rate 
    FROM 
        snowflake.organization_usage.rate_sheet_daily
    WHERE 
        usage_type = 'storage' 
    ORDER BY 
        date DESC
    LIMIT 1
)
SELECT
    id AS table_id, -- Unique table identifier 
    id != clone_group_id AS is_cloned, -- If table is a clone
    table_catalog || '.' || table_schema || '.' || table_name AS fully_qualified_table_name, -- Full name
    active_bytes,
    time_travel_bytes,
    failsafe_bytes,
    retained_for_clone_bytes,
    (active_bytes + time_travel_bytes + failsafe_bytes + retained_for_clone_bytes) / POWER(1024, 4) AS total_storage_tb, -- Storage in TBs
    current_storage_cost.effective_rate AS storage_price_per_tb,
    total_storage_tb * storage_price_per_tb AS price,
    current_storage_cost.currency AS currency
FROM 
    snowflake.account_usage.table_storage_metrics
CROSS JOIN 
    current_storage_cost
WHERE 
    NOT deleted -- Only live tables
ORDER BY 
    price DESC;
