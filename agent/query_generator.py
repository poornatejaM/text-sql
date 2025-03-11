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
                # Attempt to fix the query
                query = self._fix_query(query, schema, table_name, user_query)
            
            return query
        
        except Exception as e:
            logging.error(f"Query generation failed: {str(e)}")
            return None
    
    def _format_schema_for_prompt(self, schema: Dict[str, Any]) -> str:
        """Format schema information for inclusion in prompts."""
        schema_str = ""
        for field_name, field_info in schema.items():
            field_type = field_info.get("type", "Unknown")
            field_desc = field_info.get("description", "")
            schema_str += f"- {field_name} ({field_type}): {field_desc}\n"
        
        return schema_str
    
    def _extract_query_type(self, query: str) -> str:
        """Determine the probable type of query based on keywords."""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ["average", "avg", "sum", "count", "total", "mean", "median", "min", "max"]):
            return "aggregation"
        elif any(word in query_lower for word in ["trend", "over time", "change", "growth", "increase", "decrease"]):
            return "trend analysis"
        elif any(word in query_lower for word in ["top", "best", "worst", "highest", "lowest", "ranking"]):
            return "ranking"
        elif any(word in query_lower for word in ["compare", "versus", "vs", "against", "difference"]):
            return "comparison"
        elif any(word in query_lower for word in ["group", "by", "category", "segment", "breakdown", "split"]):
            return "grouping"
        elif any(word in query_lower for word in ["filter", "where", "only", "exclude", "include"]):
            return "filtering"
        else:
            return "general"
    
    def _clean_query(self, query: str) -> str:
        """Clean up the query by removing markdown code blocks and extra whitespace."""
        # Remove markdown code block markers if present
        query = re.sub(r'^```(?:sql)?\s*', '', query, flags=re.IGNORECASE)
        query = re.sub(r'\s*```$', '', query, flags=re.IGNORECASE)
        
        # Trim whitespace
        query = query.strip()
        
        return query
    
    def _validate_query(self, query: str, schema: Dict[str, Any]) -> bool:
        """
        Validate that the generated SQL query is well-formed and uses correct fields.
        
        Args:
            query: The SQL query to validate
            schema: The schema of the table being queried
            
        Returns:
            bool: True if the query is valid, False otherwise
        """
        if not query:
            return False
        
        # Check if the query contains basic SQL keywords
        basic_keywords = ["SELECT", "FROM"]
        if not all(keyword.upper() in query.upper() for keyword in basic_keywords):
            logging.error("Query missing basic SQL keywords (SELECT, FROM)")
            return False
        
        # Get all field names from the schema
        schema_fields = set(schema.keys())
        
        # Extract field names used in the query
        # This is a simple extraction and might not catch all cases
        # For a more robust solution, a proper SQL parser would be needed
        field_pattern = r'[^a-zA-Z0-9_]([a-zA-Z][a-zA-Z0-9_]*)[^a-zA-Z0-9_]'
        used_fields = set(re.findall(field_pattern, query))
        
        # Remove SQL keywords and functions from extracted fields
        sql_keywords = {"SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING", 
                        "JOIN", "INNER", "OUTER", "LEFT", "RIGHT", "ON", "AS", "IN", 
                        "LIKE", "IS", "NOT", "NULL", "AND", "OR", "CASE", "WHEN", 
                        "THEN", "ELSE", "END", "LIMIT", "OFFSET", "DISTINCT", "COUNT",
                        "SUM", "AVG", "MIN", "MAX", "WITH"}
        
        sql_functions = {"COUNT", "SUM", "AVG", "MIN", "MAX", "ROUND", "DATE", "EXTRACT",
                         "TO_CHAR", "TO_DATE", "CURRENT_DATE", "CURRENT_TIMESTAMP", 
                         "LOWER", "UPPER", "TRIM", "SUBSTRING", "IF", "CASE"}
        
        used_fields = used_fields - sql_keywords - sql_functions
        
        # Check if all used fields exist in the schema
        invalid_fields = used_fields - schema_fields
        
        if invalid_fields:
            logging.warning(f"Query uses non-existent fields: {', '.join(invalid_fields)}")
            return False
        
        # Check for potential SQL injection patterns
        injection_patterns = [
            r';\s*DROP', r';\s*DELETE', r';\s*UPDATE', r';\s*INSERT',
            r'--', r'/\*', r'\*/', r'UNION\s+SELECT'
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                logging.error(f"Query contains potential SQL injection pattern: {pattern}")
                return False
        
        return True
    
    def _fix_query(self, query: str, schema: Dict[str, Any], table_name: str, user_query: str) -> str:
        """
        Attempt to fix an invalid SQL query.
        
        Args:
            query: The invalid SQL query
            schema: The schema of the table being queried
            table_name: The name of the table being queried
            user_query: The original user query
            
        Returns:
            str: The fixed SQL query
        """
        try:
            # Create a prompt to fix the query
            system_prompt = f"""You are an expert in ClickHouse SQL. The following query has validation errors.
            
            Original user question: {user_query}
            
            Invalid query:
            {query}
            
            Table schema for {table_name}:
            {self._format_schema_for_prompt(schema)}
            
            Fix the SQL query to make it valid for ClickHouse. Common issues include:
            1. Using fields that don't exist in the schema
            2. Improper ClickHouse syntax
            3. Missing table name in the FROM clause
            4. Incorrect use of aggregation functions
            5. Missing GROUP BY clauses when using aggregations
            
            Return ONLY the corrected SQL query without any explanations.
            """
            
            prompt = self.make_llama_3_prompt(user_query, system_prompt)
            
            # Generate fixed query
            result = self.llm.generate(prompt, max_new_tokens=600)
            fixed_query = result if isinstance(result, str) else result.get("response", "")
            
            # Clean up the fixed query
            fixed_query = self._clean_query(fixed_query)
            
            # Validate again
            if self._validate_query(fixed_query, schema):
                logging.info("Successfully fixed query")
                return fixed_query
            else:
                # If fixing failed, generate a simple query as fallback
                logging.warning("Failed to fix query, generating simple fallback query")
                return self._generate_fallback_query(schema, table_name, user_query)
                
        except Exception as e:
            logging.error(f"Query fixing failed: {str(e)}")
            return self._generate_fallback_query(schema, table_name, user_query)
    
    def _generate_fallback_query(self, schema: Dict[str, Any], table_name: str, user_query: str) -> str:
        """
        Generate a simple fallback query when validation and fixing fail.
        
        Args:
            schema: The schema of the table being queried
            table_name: The name of the table being queried
            user_query: The original user query
            
        Returns:
            str: A simple fallback SQL query
        """
        # Get the first 5 fields from the schema
        fields = list(schema.keys())[:5]
        fields_str = ", ".join(fields)
        
        # Create a simple SELECT query
        fallback_query = f"""
        SELECT {fields_str}
        FROM {table_name}
        LIMIT 10
        """
        
        logging.info("Generated fallback query")
        return fallback_query