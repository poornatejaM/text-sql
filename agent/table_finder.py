import logging
import lamini
from typing import List, Dict, Any
import re

class TableFinder:
    """Finds relevant tables for a given query in ClickHouse database."""
    
    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config
        self.table_cache = {}
        self.debug_mode = True  # Set to False in production
        
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
    
    def get_all_tables(self) -> List[str]:
        """
        Get a list of all available tables in the database.
        
        Returns:
            List of table names
        """
        try:
            query = "SHOW TABLES"
            from agent.query_executor import QueryExecutor
            executor = QueryExecutor(self.config)
            result = executor.execute_query(query)
            
            if not result:
                raise ValueError("Failed to retrieve tables from database")
            
            return [row["name"] for row in result]
        except Exception as e:
            if self.debug_mode:
                logging.error(f"Error fetching tables: {str(e)}")
            return []

    def get_table_schema(self, table_name: str) -> str:
        """
        Get the schema of a specified table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Formatted schema as a string
        """
        if table_name in self.table_cache:
            return self.table_cache[table_name]
        
        try:
            # Get table structure from ClickHouse
            schema_query = f"DESCRIBE TABLE {table_name}"
            from agent.query_executor import QueryExecutor
            executor = QueryExecutor(self.config)
            schema_result = executor.execute_query(schema_query)
            
            if not schema_result:
                raise ValueError(f"Failed to retrieve schema for table {table_name}")
            
            # Format schema information
            schema_str = "Column Name | Type | Default | Comment | Codec | TTL\n"
            schema_str += "-----------|------|---------|---------|-------|-----\n"
            
            for row in schema_result:
                column_name = row.get("name", '')
                column_type = row.get("type", '')
                default_expr = row.get("default_expression", '')
                comment = row.get("comment", '')
                codec_expr = row.get("codec_expression", '')
                ttl_expr = row.get("ttl_expression", '')
                
                schema_str += f"{column_name} | {column_type} | {default_expr} | {comment} | {codec_expr} | {ttl_expr}\n"
            
            # Also get a sample of data
            try:
                sample_query = f"SELECT * FROM {table_name} LIMIT 3"
                sample_result = executor.execute_query(sample_query)
                
                if sample_result:
                    schema_str += f"\nSample data from {table_name}:\n"
                    cols = [col["name"] for col in schema_result]
                    schema_str += " | ".join(cols) + "\n"
                    schema_str += "-" * (len(" | ".join(cols))) + "\n"
                    
                    for row in sample_result:
                        schema_str += " | ".join([str(val) for val in row.values()]) + "\n"
            except Exception as e:
                if self.debug_mode:
                    logging.warning(f"Error getting sample data: {str(e)}")
                schema_str += f"\nUnable to fetch sample data: {str(e)}\n"
            
            # Cache the result
            self.table_cache[table_name] = schema_str
            return schema_str
        except Exception as e:
            error_msg = f"Error fetching schema for {table_name}: {str(e)}"
            if self.debug_mode:
                logging.error(error_msg)
            return error_msg

    def identify_relevant_tables(self, user_query: str) -> List[Dict[str, Any]]:
        """
        Identify relevant tables for the user query.
        
        Args:
            user_query: The user's query (rephrased)
            
        Returns:
            List of relevant table names with scores and descriptions
        """
        tables = self.get_all_tables()
        tables_with_schemas = {}
        
        # Get schema for each table
        for table in tables:
            tables_with_schemas[table] = self.get_table_schema(table)
        
        # Format all tables and their schemas for the LLM
        tables_info = ""
        for table, schema in tables_with_schemas.items():
            tables_info += f"Table: {table}\nSchema:\n{schema}\n\n"
        
        system_prompt = f"""You are a senior database expert. Based on the following tables and their schemas, 
        identify which tables are relevant to answering the user's query. 
        Only return the table names separated by commas. If multiple tables could be joined, list all relevant tables.

        {tables_info}"""
                
        prompt = self.make_llama_3_prompt(user_query, system_prompt)
        
        try:
            result = self.llm.generate(
                prompt, 
                output_type={"relevant_tables": "str"}, 
                max_new_tokens=200
            )
            
            relevant_tables_str = result.get("relevant_tables", "")
            relevant_tables = [t.strip() for t in relevant_tables_str.split(",")]
            relevant_tables = [t for t in relevant_tables if t in tables]  # Filter out non-existent tables
            
            if not relevant_tables and tables:
                # If no relevant tables found but tables exist, use the first table as a fallback
                relevant_tables = [tables[0]]
            
            if self.debug_mode:
                logging.info(f"Identified relevant tables: {', '.join(relevant_tables)}")
            
            # Fetch and return detailed information about the relevant tables
            detailed_tables = []
            for table in relevant_tables:
                schema = tables_with_schemas.get(table, {})
                detailed_tables.append({
                    "name": table,
                    "score": 1.0,  # Placeholder score
                    "description": schema  # Placeholder description
                })
            
            return detailed_tables
        except Exception as e:
            if self.debug_mode:
                logging.error(f"Error identifying tables: {str(e)}")
            # Return first table as fallback
            return [{"name": tables[0], "score": 1.0, "description": ""}] if tables else []