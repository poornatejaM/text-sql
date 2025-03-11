# agent/query_generator.py
import logging
import lamini
import re
from typing import Dict, Any, Optional

class QueryGenerator:
    """Generates SQL queries from natural language using LLMs."""
    
    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config
        
        # Initialize LLM provider
        if self.config.llm["provider"] == "lamini":
            lamini.api_key = self.config.llm["api_key"]
            self.llm = lamini.Lamini(self.config.llm["model"])
        else:
            raise ValueError(f"Unsupported LLM provider: {self.config.llm['provider']}")
    
    def make_llama_3_prompt(self, user_query: str, system: str = "") -> str:
        """Format a prompt for Llama-3 models."""
        system_prompt = f"<|start_header_id|>system<|end_header_id|>\n\n{system}<|eot_id|>" if system else ""
        return f"<|begin_of_text|>{system_prompt}<|start_header_id|>user<|end_header_id|>\n\n{user_query}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
    
    def generate_query(self, user_query: str, schema: Dict[str, Any], table_name: str) -> Optional[str]:
        """Generate a SQL query from natural language input."""
        try:
            # Format schema for prompt
            schema_str = self._format_schema_for_prompt(schema)
            
            # Extract probable query type
            query_type = self._extract_query_type(user_query)
            
            # Create prompt
            system_prompt = f"""You are a financial analyst with 15 years of experience writing SQL queries for ClickHouse database.
            
            The {table_name} table has the following schema:
            {schema_str}
            
            Write a ClickHouse SQL query to answer the following question. Follow these rules:
            
            - Use only the fields that are necessary to answer the question
            - Make sure to use proper ClickHouse SQL syntax, not SQLite or MySQL
            - Do not use SELECT * except for very simple queries
            - Always include appropriate filters to make results meaningful
            - If aggregating data, include appropriate GROUP BY clauses
            - If sorting is implied by the question, include ORDER BY clauses
            - Use appropriate LIMIT clauses to prevent excessive results
            - If the query looks for recent data, consider filtering by date fields
            - Format the SQL query for readability with proper indentation
            
            The query appears to be a {query_type} type query.
            
            Provide ONLY the SQL query as your response, with no explanations or other text.
            """
            
            prompt = self.make_llama_3_prompt(user_query, system_prompt)
            
            # Generate SQL query
            result = self.llm.generate(prompt, output_type={"clickhouse_query": "str"}, max_new_tokens=600)
            
            query = result.get("clickhouse_query", "")
            
            # Clean up the query (remove markdown code block markers if present)
            query = self._clean_query(query)
            
            # Validate the query
            if not self._validate_query(query, schema):
                logging.warning("Generated query failed validation, attempting to fix...")