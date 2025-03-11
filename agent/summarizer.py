from typing import List, Dict, Any
import logging
import lamini

class Summarizer:
    """Summarizes SQL query results using LLMs."""
    
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
    
    def generate_summary(self, user_query: str, query_output: List[Dict[str, Any]], original_query: str = None) -> str:
        """
        Generate a natural language summary of query results.
        
        Args:
            user_query: The enhanced user query
            query_output: The results of the SQL query
            original_query: The original user query before enhancement (optional)
            
        Returns:
            str: A natural language summary of the query results
        """
        try:
            # Clean and format the output for the prompt
            formatted_output = self._format_output_for_prompt(query_output)
            
            # Use original query in context if provided
            query_context = f"Original Question: {original_query}\nEnhanced Question: {user_query}" if original_query else f"Question: {user_query}"
            
            # Create prompt
            summary_prompt = f"""
            Role: You are a financial data analyst specializing in creating clear, concise summaries of SQL query results.

            Objective: Generate a clear, informative summary of the query results based on the user's question.

            Context:
            {query_context}
            SQL Results: {formatted_output}

            Instructions:
            1. Analyze the key insights from the SQL results
            2. Present the findings in a clear, concise format
            3. Highlight noteworthy trends or patterns
            4. Include specific numbers and percentages when relevant
            5. Format the response in markdown for readability

            Your summary should be comprehensive yet concise (3-5 sentences).
            """
            
            prompt = self.make_llama_3_prompt(user_query, summary_prompt)
            
            # Generate summary
            result = self.llm.generate(prompt, max_new_tokens=300)
            summary = result if isinstance(result, str) else result.get("response", "No summary generated.")
            
            # Save summary
            self._save_summary(summary, user_query)
            
            return summary
        except Exception as e:
            logging.error(f"Summary generation failed: {str(e)}")
            return f"Failed to generate summary: {str(e)}"
    
    def _format_output_for_prompt(self, query_output: List[Dict[str, Any]]) -> str:
        """Format query output for inclusion in prompts."""
        # Limit the number of rows to prevent token overflow
        max_rows = 15
        truncated = query_output[:max_rows]
        
        # Add truncation notice
        truncation_notice = f"\n[Showing {len(truncated)} of {len(query_output)} results]" if len(query_output) > max_rows else ""
        
        # Format as string
        if not truncated:
            return "No results returned."
        
        formatted = []
        # Get header from first result
        keys = list(truncated[0].keys())
        
        # Format each row
        for row in truncated:
            row_str = ", ".join([f"{k}: {row[k]}" for k in keys])
            formatted.append(f"({row_str})")
        
        return "\n".join(formatted) + truncation_notice
    
    def _save_summary(self, summary: str, user_query: str) -> None:
        """Save the generated summary for reference."""
        try:
            with open(f"{self.config.paths['output']}/last_summary.md", "w") as f:
                f.write(f"# Summary for: {user_query}\n\n{summary}")
        except Exception as e:
            logging.warning(f"Failed to save summary: {str(e)}")