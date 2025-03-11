import os
import clickhouse_connect
from dotenv import load_dotenv
import lamini

# Load environment variables
load_dotenv()

# Retrieve API key
LAMINI_API_KEY = os.getenv("LAMINI_API_KEY")

# ClickHouse Database Credentials
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
lamini.api_key = LAMINI_API_KEY

llm = lamini.Lamini("meta-llama/Meta-Llama-3.1-8B-Instruct")

# Initialize ClickHouse Client
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=True
    )
