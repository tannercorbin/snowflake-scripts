WITH current_storage_provider_cost AS (
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
), 
table_storage_cost AS (
    SELECT
        id AS table_id, -- Unique table identifier 
        id != clone_group_id AS is_cloned, -- If table is a clone
        table_catalog || '.' || table_schema || '.' || table_name AS fully_qualified_table_name, -- Full name
        active_bytes,
        time_travel_bytes,
        failsafe_bytes,
        retained_for_clone_bytes,
        (active_bytes + time_travel_bytes + failsafe_bytes + retained_for_clone_bytes) / POWER(1024, 4) AS total_storage_tb, -- Storage in TBs
        current_storage_provider_cost.effective_rate AS storage_price_per_tb,
        total_storage_tb * storage_price_per_tb AS price,
        current_storage_provider_cost.currency AS currency
    FROM 
        snowflake.account_usage.table_storage_metrics
    CROSS JOIN 
        current_storage_provider_cost
    WHERE 
        NOT deleted -- Only live tables
), 
table_dml_details AS (
    SELECT
        objects_modified.value:objectId::INTEGER AS table_id,
        COUNT(*) AS num_times_dml,
        MAX(query_start_time) AS last_dml,
        TIMEDIFF(DAYS, last_dml, CURRENT_TIMESTAMP()) AS num_days_past_last_dml
    FROM 
        snowflake.account_usage.access_history, 
        LATERAL FLATTEN(snowflake.account_usage.access_history.objects_modified) AS objects_modified
    WHERE 
        objects_modified.value:objectDomain::TEXT = 'Table' 
        AND table_id IS NOT NULL
        AND query_start_time > DATEADD('days', -30, CURRENT_DATE)
    GROUP BY 
        table_id
), 
table_ddl_details AS (
    SELECT
        object_modified_by_ddl:objectId::INTEGER AS table_id,
        COUNT(*) AS num_times_ddl,
        MAX(query_start_time) AS last_ddl,
        TIMEDIFF(DAYS, last_ddl, CURRENT_TIMESTAMP()) AS num_days_past_last_ddl
    FROM 
        snowflake.account_usage.access_history
    WHERE 
        object_modified_by_ddl:objectDomain::TEXT = 'Table' 
        AND table_id IS NOT NULL
        AND query_start_time > DATEADD('days', -30, CURRENT_DATE)
    GROUP BY 
        table_id
), 
table_access_details AS (
    SELECT
        objects_accessed.value:objectId::INTEGER AS table_id, -- Will be null for secured views or tables from a data share
        COUNT(*) AS num_times_access,
        MAX(query_start_time) AS last_access_time,
        TIMEDIFF(DAYS, last_access_time, CURRENT_TIMESTAMP()) AS num_days_past_access
    FROM 
        snowflake.account_usage.access_history, 
        LATERAL FLATTEN(snowflake.account_usage.access_history.base_objects_accessed) AS objects_accessed
    WHERE 
        objects_accessed.value:objectDomain::TEXT = 'Table' 
        AND table_id IS NOT NULL
        AND query_start_time > DATEADD('days', -30, CURRENT_DATE)
    GROUP BY 
        table_id
)
SELECT 
    table_storage_cost.table_id, 
    fully_qualified_table_name, 
    active_bytes, 
    time_travel_bytes, 
    failsafe_bytes, 
    retained_for_clone_bytes, 
    total_storage_tb, 
    storage_price_per_tb, 
    price, 
    currency, 
    num_times_dml, 
    last_dml, 
    num_days_past_last_dml, 
    num_times_ddl, 
    last_ddl, 
    num_days_past_last_ddl, 
    num_times_access, 
    last_access_time, 
    num_days_past_access  
FROM 
    table_storage_cost
LEFT OUTER JOIN 
    table_dml_details ON table_storage_cost.table_id = table_dml_details.table_id
LEFT OUTER JOIN 
    table_ddl_details ON table_storage_cost.table_id = table_ddl_details.table_id
LEFT OUTER JOIN 
    table_access_details ON table_storage_cost.table_id = table_access_details.table_id
ORDER BY 
    CASE WHEN num_days_past_access IS NULL THEN 1 ELSE 0 END DESC,
    num_days_past_access DESC, 
    price DESC;
