# agent/table_finder.py
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
    
    def find_relevant_tables(self, query: str) -> List[Dict[str, Any]]:
        """
        Find relevant tables for a given query.
        
        Args:
            query: The natural language query
            
        Returns:
            List of dictionaries with table information, sorted by relevance
        """
        try:
            # Get all tables if not already cached
            if not self.table_cache:
                self._populate_table_cache()
            
            if not self.table_cache:
                logging.warning("No tables found in database or failed to retrieve tables")
                return []
            
            # Use LLM to rank tables by relevance
            relevant_tables = self._rank_tables_by_relevance(query)
            
            return relevant_tables
        
        except Exception as e:
            logging.error(f"Failed to find relevant tables: {str(e)}")
            return []
    
    def _populate_table_cache(self):
        """Populate cache with tables from the database."""
        from agent.query_executor import QueryExecutor
        
        # Create a query executor
        executor = QueryExecutor(self.config)
        
        # Query to get all tables
        query = """
        SELECT 
            database,
            name,
            engine,
            total_rows,
            total_bytes,
            comment
        FROM 
            system.tables
        WHERE 
            database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        """
        
        # Execute query
        result = executor.execute_query(query)
        
        if not result:
            logging.error("Failed to retrieve tables from database")
            return
        
        # Populate cache
        for table in result:
            table_name = table["name"]
            self.table_cache[table_name] = {
                "database": table["database"],
                "name": table_name,
                "engine": table["engine"],
                "rows": table["total_rows"],
                "size_bytes": table["total_bytes"],
                "description": table.get("comment", "")
            }
        
        # For each table, get column information
        for table_name in self.table_cache:
            self._get_table_columns(table_name)
    
    def _get_table_columns(self, table_name: str):
        """Get column information for a table."""
        from agent.query_executor import QueryExecutor
        
        # Create a query executor
        executor = QueryExecutor(self.config)
        
        # Query to get column information
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
        
        if result:
            self.table_cache[table_name]["columns"] = [
                {
                    "name": col["name"],
                    "type": col["type"],
                    "description": col.get("comment", "")
                }
                for col in result
            ]
    
    def _rank_tables_by_relevance(self, query: str) -> List[Dict[str, Any]]:
        """Rank tables by relevance to the query using LLM."""
        # If there are more than 10 tables, use LLM to rank them
        if len(self.table_cache) > 10:
            return self._rank_tables_with_llm(query)
        else:
            # For fewer tables, rank them using a simpler heuristic approach
            return self._rank_tables_with_heuristics(query)
    
    def _rank_tables_with_heuristics(self, query: str) -> List[Dict[str, Any]]:
        """Rank tables using simple keyword matching heuristics."""
        # Extract potential keywords from the query
        keywords = set(re.findall(r'\b\w+\b', query.lower()))
        
        # Remove common stop words
        stop_words = {'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'was', 
                     'were', 'be', 'been', 'being', 'in', 'on', 'at', 'to', 'for',
                     'with', 'by', 'about', 'against', 'between', 'into', 'through',
                     'during', 'before', 'after', 'above', 'below', 'from', 'up',
                     'down', 'of', 'off', 'over', 'under', 'again', 'further', 'then',
                     'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all',
                     'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some',
                     'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than',
                     'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should',
                     'now', 'what', 'which', 'who', 'whom', 'this', 'that', 'these',
                     'those', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
                     'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his',
                     'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
                     'they', 'them', 'their', 'theirs', 'themselves', 'show', 'get', 'find'}
        
        keywords -= stop_words
        
        # Score tables based on keyword matches
        table_scores = []
        
        for table_name, table_info in self.table_cache.items():
            score = 0
            
            # Check table name
            table_words = set(re.findall(r'\b\w+\b', table_name.lower()))
            score += len(keywords.intersection(table_words)) * 2
            
            # Check table description
            if table_info.get("description"):
                desc_words = set(re.findall(r'\b\w+\b', table_info["description"].lower()))
                score += len(keywords.intersection(desc_words))
            
            # Check column names and descriptions
            for column in table_info.get("columns", []):
                col_name = column["name"].lower()
                col_words = set(re.findall(r'\b\w+\b', col_name))
                score += len(keywords.intersection(col_words)) * 1.5
                
                if column.get("description"):
                    col_desc_words = set(re.findall(r'\b\w+\b', column["description"].lower()))
                    score += len(keywords.intersection(col_desc_words)) * 0.5
            
            # Bonus for larger tables with actual data
            if table_info.get("rows", 0) > 100:
                score += 0.5
            
            # Add to results
            table_scores.append({
                "name": table_name,
                "score": score,
                "description": table_info.get("description", ""),
                "columns": len(table_info.get("columns", [])),
                "rows": table_info.get("rows", 0)
            })
        
        # Sort by score (descending)
        table_scores.sort(key=lambda x: x["score"], reverse=True)
        
        # If default table exists and has a reasonable score, prioritize it
        default_table = self.config.default_table
        if default_table:
            for i, table in enumerate(table_scores):
                if table["name"] == default_table and table["score"] > 0:
                    # Move to top if has some relevance
                    if i > 0:
                        default_table_info = table_scores.pop(i)
                        table_scores.insert(0, default_table_info)
                    break
        
        # Add the default table if it wasn't found but exists in cache
        if default_table and default_table in self.table_cache and not any(t["name"] == default_table for t in table_scores):
            default_info = self.table_cache[default_table]
            table_scores.insert(0, {
                "name": default_table,
                "score": 0.5,  # Low but non-zero score
                "description": default_info.get("description", "Default table"),
                "columns": len(default_info.get("columns", [])),
                "rows": default_info.get("rows", 0)
            })
        
        # Return all tables with a score > 0, or at least one table
        result = [t for t in table_scores if t["score"] > 0]
        return result if result else table_scores[:1]
    
    def _rank_tables_with_llm(self, query: str) -> List[Dict[str, Any]]:
        """Rank tables by relevance using LLM."""
        # Prepare table information for the prompt
        table_info_str = ""
        for table_name, info in self.table_cache.items():
            table_info_str += f"Table: {table_name}\n"
            table_info_str += f"Description: {info.get('description', 'N/A')}\n"
            table_info_str += "Columns:\n"
            
            # Add column information
            for col in info.get("columns", []):
                col_desc = col.get("description", "").strip()
                col_info = f"- {col['name']} ({col['type']})"
                if col_desc:
                    col_info += f": {col_desc}"
                table_info_str += f"{col_info}\n"
            
            table_info_str += "\n"
        
        # Create prompt
        system_prompt = f"""You are a database expert who can identify which tables are most relevant to a natural language query.
        
        Given the following database tables and a user query, rank the top 5 most relevant tables for answering the query.
        
        Database Tables:
        {table_info_str}
        
        For each table, provide:
        1. The table name
        2. A relevance score from 0-10 (10 being most relevant)
        3. A brief explanation of why the table is relevant
        
        Format your response as a JSON array of objects with the following structure:
        [
            {{"name": "table_name", "score": 8.5, "reason": "This table contains..."}},
            ...
        ]
        
        Only include tables that have some relevance to the query (score > 0).
        """
        
        prompt = self.make_llama_3_prompt(query, system_prompt)
        
        # Generate rankings
        result = self.llm.generate(prompt, output_type={"rankings": "list[dict]"}, max_new_tokens=800)
        
        try:
            rankings = result.get("rankings", [])
            
            if not rankings:
                logging.warning("LLM did not return valid table rankings, using heuristic method instead")
                return self._rank_tables_with_heuristics(query)
            
            # Format the results
            formatted_rankings = []
            for rank in rankings:
                table_name = rank.get("name", "").strip()
                if table_name in self.table_cache:
                    formatted_rankings.append({
                        "name": table_name,
                        "score": float(rank.get("score", 0)),
                        "description": rank.get("reason", ""),
                        "columns": len(self.table_cache[table_name].get("columns", [])),
                        "rows": self.table_cache[table_name].get("rows", 0)
                    })
            
            # Sort by score (descending)
            formatted_rankings.sort(key=lambda x: x["score"], reverse=True)
            
            # If no tables were found as relevant, use heuristic method
            if not formatted_rankings:
                return self._rank_tables_with_heuristics(query)
            
            return formatted_rankings
        
        except Exception as e:
            logging.error(f"Failed to parse LLM table rankings: {str(e)}")
            return self._rank_tables_with_heuristics(query)