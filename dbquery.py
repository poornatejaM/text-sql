import pandas as pd
from agent.config_settings import get_clickhouse_client

# Connect to ClickHouse
client = get_clickhouse_client()

query = "SELECT Product_Category FROM sales_data GROUP BY Product_Category ORDER BY COUNT(Product_Category) DESC LIMIT 1"
df = pd.DataFrame(client.query_df(query))
print(df.head())