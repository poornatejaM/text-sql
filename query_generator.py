import lamini
from config_settings import LAMINI_API_KEY

# Initialize Lamini API
lamini.api_key = LAMINI_API_KEY
llm = lamini.Lamini("meta-llama/Meta-Llama-3.1-8B-Instruct")

# Generates the Llama-3 formatted prompt
def make_llama_3_prompt(user_query, system=""):
    system_prompt = f"<|start_header_id|>system<|end_header_id|>\n\n{system}<|eot_id|>" if system else ""
    return f"<|begin_of_text|>{system_prompt}<|start_header_id|>user<|end_header_id|>\n\n{user_query}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"

# SQL Schema
def get_updated_schema():
    return """
    'Product_ID', 'Int64', '', '', '', '', '\nSale_Date', 'Date', '', '', '', '', '\nSales_Rep', 'String', '', '', '', '',
    '\nRegion', 'String', '', '', '', '', '\nSales_Amount', 'Float64', '', '', '', '', '\nQuantity_Sold', 'Int64', '', '', '', '',
    '\nProduct_Category', 'String', '', '', '', '', '\nUnit_Cost', 'Float64', '', '', '', '', '\nUnit_Price', 'Float64', '', '', '', '',
    '\nCustomer_Type', 'String', '', '', '', '', '\nDiscount', 'Float64', '', '', '', '', '\nPayment_Method', 'String', '', '', '', '',
    '\nSales_Channel', 'String', '', '', '', '', '\nRegion_and_Sales_Rep', 'String', '', '', '', '', ''
    """

# Generate SQL query from user input
def generate_query(user_query):
    system_prompt = f"""You are a financial analyst with 15 years of experience writing SQL queries. 
    The sales_data table has the following schema:
    {get_updated_schema()}
    Write a SQL query to answer the following question:"""

    prompt = make_llama_3_prompt(user_query, system_prompt)
    result = llm.generate(prompt, output_type={"sqlite_query": "str"}, max_new_tokens=200)

    return result.get("sqlite_query", "")
