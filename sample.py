import os
import lamini
import clickhouse_connect
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve API key from environment variables
lamini.api_key = os.getenv("LAMINI_API_KEY")

# Initialize the LLM model
llm = lamini.Lamini("meta-llama/Meta-Llama-3.1-8B-Instruct")

def make_llama_3_prompt(user, system=""):
    system_prompt = f"<|start_header_id|>system<|end_header_id|>\n\n{system}<|eot_id|>" if system else ""
    return f"<|begin_of_text|>{system_prompt}<|start_header_id|>user<|end_header_id|>\n\n{user}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"

def get_updated_schema():
    return """
'Product_ID', 'Int64', '', '', '', '', '\nSale_Date', 'Date', '', '', '', '', '\nSales_Rep', 'String', '', '', '', '', '\nRegion', 'String', '', '', '', '', '\nSales_Amount', 'Float64', '', '', '', '', '\nQuantity_Sold', 'Int64', '', '', '', '', '\nProduct_Category', 'String', '', '', '', '', '\nUnit_Cost', 'Float64', '', '', '', '', '\nUnit_Price', 'Float64', '', '', '', '', '\nCustomer_Type', 'String', '', '', '', '', '\nDiscount', 'Float64', '', '', '', '', '\nPayment_Method', 'String', '', '', '', '', '\nSales_Channel', 'String', '', '', '', '', '\nRegion_and_Sales_Rep', 'String', '', '', '', '', ''
"""

def generate_summary(user, query_output):
    summary_prompt = f"""
    Role: You summarize SQL query results concisely based on the user's query.

    Objective: Generate a clear and relevant response using SQL output.
    
    Context:
    User Query: {user}
    SQL Output: {query_output}
    
    Constraints:
    Ensure the response is concise and directly answers the query.
    If insights or predictions can be derived, include them in the next line; otherwise, omit them.
    
    Output Format:
    A one- or two-sentence answer based on {query_output} that aligns with {user}.
    """
    prompt = make_llama_3_prompt(user, summary_prompt)
    result = llm.generate(prompt, max_new_tokens=200)
    return result

user = "which product category sold most"

system = f"""You are a financial analyst with 15 years of experience writing complex SQL queries. Consider the sales_data table with the following schema:
{get_updated_schema()}

Write a SQL query to answer the following question. Follow instructions exactly."""

prompt = make_llama_3_prompt(user, system)
result = llm.generate(prompt, output_type={"sqlite_query": "str"}, max_new_tokens=200)
print(result)
query = result.get("sqlite_query", "")

if query:
    client = clickhouse_connect.get_client(
        host='n0barcb92d.ap-south-1.aws.clickhouse.cloud',
        user='default',
        password='zT05UExfnB~3F',
        secure=True
    )
    response = client.query(query)
    query_output = response.result_set
    print("User Query:", user)
    print("Generated Query:", query)
    print("Query Result:", query_output)
    
    summary = generate_summary(user, query_output)
    print("Summary:", summary)
else:
    print("No query generated.")
