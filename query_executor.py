import json
import logging
from config_settings import get_clickhouse_client

# Configure logging
logging.basicConfig(filename="logs/app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def execute_query(query):
    client = get_clickhouse_client()

    try:
        response = client.query(query)
        result_set = response.result_set
        logging.info(f"Executed Query: {query}")
        
        # Store query for reference
        with open("sql_queries/last_query.sql", "w") as f:
            f.write(query)
        
        # Store results in JSON format
        with open("output/query_result.json", "w") as f:
            json.dump(result_set, f, indent=4)
        
        return result_set
    except Exception as e:
        logging.error(f"Query Execution Failed: {str(e)}")
        return None
