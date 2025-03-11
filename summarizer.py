import lamini
from query_generator import make_llama_3_prompt
from config_settings import llm 

def generate_summary(user_query, query_output):
    summary_prompt = f"""
    Role: You summarize SQL query results concisely based on the user's query.

    Objective: Generate a clear and relevant response using SQL output.

    Context:
    User Query: {user_query}
    SQL Output: {query_output}

    Constraints:
    Ensure the response is concise and directly answers the query.
    If insights or predictions can be derived, include them; otherwise, omit them.

    Output Format:
    A one- or two-sentence summary.
    """
    prompt = make_llama_3_prompt(user_query, summary_prompt)
    result = llm.generate(prompt, max_new_tokens=200)
    return result
