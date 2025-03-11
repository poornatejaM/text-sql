import logging
from typing import Dict, Any, Optional

class SchemaManager:
    """Manages database schema information."""
    
    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config
        self.schema_cache = {}
    
    def get_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get schema for a specific table."""
        # Check cache first
        if table_name in self.schema_cache:
            return self.schema_cache[table_name]
        
        try:
            # If table is sales_data, use the default schema
            if table_name == "sales_data":
                schema = self._get_sales_data_schema()
            else:
                # For other tables, try to fetch schema from database
                schema = self._fetch_schema_from_db(table_name)
            
            # Cache the schema
            self.schema_cache[table_name] = schema
            return schema
        
        except Exception as e:
            logging.error(f"Failed to get schema for {table_name}: {str(e)}")
            return None
    
    def _get_sales_data_schema(self) -> Dict[str, Any]:
        """Get hardcoded schema for sales_data table."""
        return {
            "Product_ID": {"type": "Int64", "description": "Unique identifier for products"},
            "Sale_Date": {"type": "Date", "description": "Date of the sale"},
            "Sales_Rep": {"type": "String", "description": "Name of the sales representative"},
            "Region": {"type": "String", "description": "Geographic region of the sale"},
            "Sales_Amount": {"type": "Float64", "description": "Total amount of the sale"},
            "Quantity_Sold": {"type": "Int64", "description": "Number of units sold"},
            "Product_Category": {"type": "String", "description": "Category of the product"},
            "Unit_Cost": {"type": "Float64", "description": "Cost per unit"},
            "Unit_Price": {"type": "Float64", "description": "Price per unit"},
            "Customer_Type": {"type": "String", "description": "Type of customer (Retail, Wholesale, etc.)"},
            "Discount": {"type": "Float64", "description": "Discount percentage applied"},
            "Payment_Method": {"type": "String", "description": "Method of payment"},
            "Sales_Channel": {"type": "String", "description": "Channel through which sale was made"},
            "Region_and_Sales_Rep": {"type": "String", "description": "Combination of region and sales rep"}
        }
    
    def _fetch_schema_from_db(self, table_name: str) -> Dict[str, Any]:
        """Fetch schema information from database."""
        from agent.query_executor import QueryExecutor
        
        # Create a query executor
        executor = QueryExecutor(self.config)
        
        # Query to get schema information
        query = f"""
        SELECT 
            name, 
            type,
            comment
        FROM 
            system.columns
        WHERE 
            table = '{table_name}'
        """
        
        # Execute query
        result = executor.execute_query(query)
        
        if not result:
            raise ValueError(f"Could not retrieve schema for table {table_name}")
        
        # Convert result to schema dictionary
        schema = {}
        for row in result:
            schema[row['name']] = {
                "type": row['type'],
                "description": row.get('comment', '')
            }
        
        return schema