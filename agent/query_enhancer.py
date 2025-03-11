# agent/query_enhancer.py
import logging
import lamini
from typing import Tuple

class QueryEnhancer:
    """Enhances and rephrases user queries for better understanding and accuracy."""
    
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
    
    def enhance_query(self, user_query: str) -> Tuple[str, str]:
        """
        Enhance and rephrase user query for better understanding and accuracy.
        
        Returns:
            tuple: (enhanced_query, original_query)
        """
        try:
            # Store original query
            original_query = user_query
            
            # Don't enhance if already well-formed
            if len(user_query.split()) > 10 and any(word in user_query.lower() for word in ["select", "from", "where", "group", "order"]):
                return user_query, original_query
            
            # Create prompt for query enhancement
            system_prompt = """You are an expert data analyst specializing in transforming ambiguous or incomplete 
            data queries into clear, specific questions. Your task is to rephrase the user's query into a more 
            precise and comprehensive question that will lead to more accurate SQL query generation.

            Guidelines:
            1. Maintain the original intent and meaning
            2. Add specificity where the query is ambiguous
            3. Expand abbreviated terms or jargon into their full forms
            4. Structure the question to clearly indicate what data is being requested
            5. DO NOT add made-up constraints or filters that weren't in the original query
            6. Provide ONLY the enhanced query as your response, with no explanations or notes

            Example:
            User: "top products last month"
            You: "What were the top-selling products based on sales volume during the last month?"
            """
            
            prompt = self.make_llama_3_prompt(user_query, system_prompt)
            
            # Generate enhanced query
            result = self.llm.generate(prompt, max_new_tokens=150)
            
            enhanced_query = result.strip() if isinstance(result, str) else result.get("response", "").strip()
            
            # Sanity check - if the enhanced query is empty or too different, use original
            if not enhanced_query or len(enhanced_query) < 10:
                return user_query, original_query
            
            # Log the enhancement
            logging.info(f"Enhanced query: '{user_query}' -> '{enhanced_query}'")
            
            return enhanced_query, original_query
        
        except Exception as e:
            logging.error(f"Query enhancement failed: {str(e)}")
            # Return original if enhancement fails
            return user_query, user_query