from query_generator import generate_query
from query_executor import execute_query
from summarizer import generate_summary

def main():
    user_query = "which product category sold most"
    query = generate_query(user_query)
    
    if query:
        print("Generated Query:", query)
        query_output = execute_query(query)
        
        if query_output:
            print("Query Result:", query_output)
            
            # Summarize results
            summary = generate_summary(user_query, query_output)
            print("Summary:", summary)
        else:
            print("Query execution failed.")
    else:
        print("No query generated.")

if __name__ == "__main__":
    main()
