import json
import logging
import clickhouse_connect
from typing import Optional, List, Dict, Any

class QueryExecutor:
    """Executes SQL queries against a ClickHouse database."""
    
    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config
        self.debug_mode = True  # Set to False in production
    
    def get_clickhouse_client(self):
        """Get a ClickHouse client connection."""
        return clickhouse_connect.get_client(
            host=self.config.database["host"],
            user=self.config.database["user"],
            password=self.config.database["password"],
            secure=self.config.database["secure"]
        )
    
    def execute_query(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """Execute a SQL query and return the results."""
        try:
            # Get client connection
            client = self.get_clickhouse_client()
            
            # Execute query
            response = client.query(query)
            
            # Convert to list of dictionaries
            result = self._convert_to_dict_list(response)
            
            # Log query execution
            logging.info(f"Executed Query: {query}")
            
            # Save results
            self._save_results(result)
            
            return result
        except Exception as e:
            logging.error(f"Query Execution Failed: {str(e)}")
            return None
    
    def _convert_to_dict_list(self, response):
        """Convert ClickHouse response to a list of dictionaries."""
        if not response or not response.result_set:
            if self.debug_mode:
                logging.warning("Empty result set from query execution.")
            return []
        
        result_set = response.result_set
        column_names = response.column_names
        
        if self.debug_mode:
            logging.debug(f"Column Names: {column_names}")
            logging.debug(f"Result Set: {result_set}")
        
        # Convert to list of dictionaries
        results = []
        for row in result_set:
            if not isinstance(row, (tuple, list)):
                logging.error(f"Unexpected row type: {type(row)}. Expected tuple or list.")
                continue
            
            row_dict = {}
            for i, col_name in enumerate(column_names):
                if i < len(row):
                    row_dict[col_name] = row[i]
                else:
                    logging.warning(f"Row length mismatch: {len(row)} columns, expected {len(column_names)} columns.")
                    row_dict[col_name] = None  # Handle mismatch by setting to None
            
            results.append(row_dict)
        
        return results
    
    def _save_results(self, results: List[Dict[str, Any]]) -> None:
        """Save query results to a file."""
        try:
            with open(f"{self.config.paths['output']}/query_result.json", "w") as f:
                # Convert datetime and date objects to strings for JSON serialization
                json.dump(results, f, indent=4, default=str)
        except Exception as e:
            logging.warning(f"Failed to save results: {str(e)}")