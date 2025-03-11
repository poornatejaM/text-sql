import lamini
from config_settings import LAMINI_API_KEY
from utils.prompt import make_llama_3_prompt
from utils.schema import get_updated_schema

# Initialize Lamini API
lamini.api_key = LAMINI_API_KEY
llm = lamini.Lamini("meta-llama/Meta-Llama-3.1-8B-Instruct")

# Generate SQL query from user input
def generate_query(user_query):
    system_prompt = f"""You are a financial analyst with 15 years of experience writing SQL queries. 
    The sales_data table has the following schema:
    {get_updated_schema()}
    Write a SQL query to answer the following question:"""

    prompt = make_llama_3_prompt(user_query, system_prompt)
    result = llm.generate(prompt, output_type={"sqlite_query": "str"}, max_new_tokens=200)

    return result.get("sqlite_query", "")
