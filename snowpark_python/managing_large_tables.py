#Source: https://medium.com/@sahil_singla/how-to-cut-storage-costs-in-snowflake-017ed8bd730f

# Purpose: This script assists in identifying and reducing under-utilized storage in Snowflake, 
# especially beneficial for large tables. It leverages Snowflake's Snowpark and SQL capabilities.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd
import numpy as np

def main(session: snowpark.Session):
    # Replace with your target table's fully qualified name
    table_name = "<database>.<schema>.<table>"  
    
    # SQL query to retrieve the most recent query IDs where the specified table was accessed.
    # The 14-day filter is applied as query operator stats are available only for this duration.
    # A limit of 1000 queries can be removed, here it is set because on X-Small, 
    # it takes around 5-6 minutes to run due to slow execution of GET_QUERY_OPERATOR_STATS
    query = f"""
            WITH relevant_queries AS (
                SELECT DISTINCT
                    query_id,
                    query_start_time
                FROM 
                    snowflake.account_usage.access_history,
                    LATERAL FLATTEN(snowflake.account_usage.access_history.base_objects_accessed) AS objects_accessed
                WHERE 
                    objects_accessed.value:objectDomain::TEXT = 'Table'
                    AND objects_accessed.value:objectName::TEXT = '{table_name}'
                    AND query_start_time > TIMEADD('days', -14, current_timestamp())
                ORDER BY 
                    query_start_time DESC
                LIMIT 1000
            )
            SELECT 
                relevant_queries.query_id 
            FROM 
                relevant_queries 
            INNER JOIN 
                snowflake.account_usage.query_history ON relevant_queries.query_id = query_history.query_id
            WHERE 
                query_history.start_time > TIMEADD('days', -14, current_timestamp())
                AND query_history.execution_status = 'SUCCESS'
            """

    # Execution of the SQL query and storing the results in a Pandas DataFrame
    df = session.sql(query).to_pandas()
    scanned_percentages = np.array([])
    
    # Analyze each query to determine the percentage of partitions scanned.
    # This helps in identifying underutilized partitions of the table.
    for i, row in df.iterrows():
        query_id = row['QUERY_ID']
        query_stats = session.sql(f"""
            SELECT 
                operator_statistics:pruning:partitions_scanned AS partitions_scanned,
                operator_statistics:pruning:partitions_total AS partitions_total,
                100 * div0(partitions_scanned, partitions_total) AS percent_scanned
            FROM 
                TABLE(GET_QUERY_OPERATOR_STATS('{query_id}')) 
            WHERE 
                operator_type = 'TableScan' AND
                operator_attributes:table_name::TEXT = '{table_name}'
        """).to_pandas()

        # Append the percentages of scanned partitions for further analysis.
        if not query_stats.empty:
            scanned_percentages = np.append(scanned_percentages, query_stats['PERCENT_SCANNED'].values)
    
    # Calculation of percentile values to understand data access patterns.
    percentiles = [0.1, 0.5, 0.9, 0.99, 1]
    percentile_values = np.percentile(scanned_percentages, [p * 100 for p in percentiles])
    
    # Organize the percentile data into a DataFrame for easy interpretation.
    # This DataFrame provides insights into the usage of partitions in the table.
    percentile_data = {'table_name': table_name}
    for i, p in enumerate(percentiles):
        percentile_data[f'p{int(p * 100)}'] = percentile_values[i]

    scanned_partitions_per_table = pd.DataFrame([percentile_data])
    
    # The resulting DataFrame is returned and can be viewed in the Results tab.
    # It offers a clear overview of partition usage, aiding in storage optimization.
    return session.create_dataframe(scanned_partitions_per_table)
