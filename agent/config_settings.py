import os
import yaml
import logging
from dotenv import load_dotenv

class Config:
    """Configuration management for the SQL Agent."""
    
    def __init__(self, config_path=None):
        """Initialize configuration from file and environment variables."""
        # Load environment variables
        load_dotenv()
        
        # Default configuration
        self.default_config = {
            "database": {
                "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
                "user": os.getenv("CLICKHOUSE_USER", "default"),
                "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
                "secure": True
            },
            "llm": {
                "provider": os.getenv("LLM_PROVIDER", "lamini"),
                "api_key": os.getenv("LAMINI_API_KEY", ""),
                "model": os.getenv("LLM_MODEL", "meta-llama/Meta-Llama-3.1-8B-Instruct")
            },
            "paths": {
                "logs": "logs",
                "output": "output",
                "sql_queries": "sql_queries"
            },
            "default_table": "sales_data"
        }
        
        # Load configuration from file if provided
        self.config = self.default_config.copy()
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    file_config = yaml.safe_load(f)
                    self._merge_configs(self.config, file_config)
            except Exception as e:
                logging.warning(f"Failed to load config from {config_path}: {str(e)}")
        
        # Create required directories
        self._create_directories()
    
    def _merge_configs(self, target, source):
        """Recursively merge source dict into target dict."""
        for key, value in source.items():
            if isinstance(value, dict) and key in target and isinstance(target[key], dict):
                self._merge_configs(target[key], value)
            else:
                target[key] = value
    
    def _create_directories(self):
        """Create required directories if they don't exist."""
        for path_key, path_value in self.config['paths'].items():
            os.makedirs(path_value, exist_ok=True)
    
    def __getattr__(self, name):
        """Allow direct access to config properties."""
        if name in self.config:
            return self.config[name]
        
        # Check nested dictionaries
        for section in self.config.values():
            if isinstance(section, dict) and name in section:
                return section[name]
        
        raise AttributeError(f"Config has no attribute '{name}'")