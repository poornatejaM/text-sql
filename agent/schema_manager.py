import logging
from typing import Dict, Any, Optional

class SchemaManager:
    """Manages database schema information."""
    
    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config
        self.schema_cache = {}
        self.debug_mode = True  # Set to False in production
    
    def make_llama_3_prompt(self, user_query: str, system: str = "") -> str:
        """Format a prompt for Llama-3 models."""
        system_prompt = f"<|start_header_id|>system<|end_header_id|>\n\n{system}<|eot_id|>" if system else ""
        return f"<|begin_of_text|>{system_prompt}<|start_header_id|>user<|end_header_id|>\n\n{user_query}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get the schema of a specified table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing the schema
        """
        if table_name in self.schema_cache:
            return self.schema_cache[table_name]
        
        try:
            # Get table structure from ClickHouse
            schema_query = f"DESCRIBE TABLE {table_name}"
            from agent.query_executor import QueryExecutor
            executor = QueryExecutor(self.config)
            schema_result = executor.execute_query(schema_query)
            
            if not schema_result:
                raise ValueError(f"Failed to retrieve schema for table {table_name}")
            
            # Format schema information
            schema = {}
            for row in schema_result:
                column_name = row.get("name", '')
                column_type = row.get("type", '')
                default_expr = row.get("default_expression", '')
                comment = row.get("comment", '')
                codec_expr = row.get("codec_expression", '')
                ttl_expr = row.get("ttl_expression", '')
                
                schema[column_name] = {
                    "type": column_type,
                    "default_expression": default_expr,
                    "comment": comment,
                    "codec_expression": codec_expr,
                    "ttl_expression": ttl_expr
                }
            
            # Also get a sample of data
            try:
                sample_query = f"SELECT * FROM {table_name} LIMIT 3"
                sample_result = executor.execute_query(sample_query)
                
                if sample_result:
                    schema["sample_data"] = sample_result
            except Exception as e:
                if self.debug_mode:
                    logging.warning(f"Error getting sample data: {str(e)}")
                schema["sample_data"] = f"Unable to fetch sample data: {str(e)}"
            
            # Cache the result
            self.schema_cache[table_name] = schema
            return schema
        except Exception as e:
            error_msg = f"Error fetching schema for {table_name}: {str(e)}"
            if self.debug_mode:
                logging.error(error_msg)
            return {"error": error_msg}