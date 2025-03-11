# app.py - Main entry point
import argparse
import logging
import time
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.markdown import Markdown
from rich.progress import Progress
from rich.table import Table

from agent.query_generator import QueryGenerator
from agent.query_executor import QueryExecutor
from agent.summarizer import Summarizer
from agent.config_settings import Config
from agent.schema_manager import SchemaManager
from agent.query_enhancer import QueryEnhancer
from agent.table_finder import TableFinder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger("rich")

# Initialize console for rich output
console = Console()

class SQLAgent:
    """Agent that handles natural language queries to a ClickHouse database."""
    
    def __init__(self, config_path=None):
        """Initialize the agent with configuration."""
        # Load configuration
        self.config = Config(config_path)
        
        # Initialize components
        self.schema_manager = SchemaManager(self.config)
        self.query_enhancer = QueryEnhancer(self.config)
        self.table_finder = TableFinder(self.config)
        self.query_generator = QueryGenerator(self.config)
        self.query_executor = QueryExecutor(self.config)
        self.summarizer = Summarizer(self.config)
        
        # Max retry attempts
        self.max_retries = 3
    
    def process_query(self, user_query, table_name=None):
        """Process a natural language query and return results."""
        console.print(Panel.fit("[bold blue]Processing Query...[/bold blue]"))
        
        # Step 1: Enhance/rephrase the user query
        console.print("[bold]Enhancing query for better understanding...[/bold]")
        enhanced_query, original_query = self.query_enhancer.enhance_query(user_query)
        if enhanced_query != user_query:
            console.print(f"[dim]Original: {user_query}[/dim]")
            console.print(f"[green]Enhanced: {enhanced_query}[/green]")
        
        # Step 2: Find relevant tables if none specified
        if not table_name:
            console.print("[bold]Finding relevant tables...[/bold]")
            with Progress() as progress:
                task = progress.add_task("[cyan]Searching tables...", total=100)
                
                # Simulate progress while task runs in background
                relevant_tables = self.table_finder.find_relevant_tables(enhanced_query)
                
                # Complete progress
                progress.update(task, completed=100)
            
            if relevant_tables:
                console.print(f"[green]Found {len(relevant_tables)} relevant tables:[/green]")
                table = Table(show_header=True)
                table.add_column("Table Name")
                table.add_column("Relevance Score")
                table.add_column("Description")
                
                for table_info in relevant_tables[:5]:  # Show top 5
                    table.add_row(
                        table_info["name"],
                        f"{table_info['score']:.2f}",
                        table_info.get("description", "")
                    )
                
                console.print(table)
                
                # Use the most relevant table
                table_name = relevant_tables[0]["name"]
                console.print(f"[bold]Using most relevant table: [cyan]{table_name}[/cyan][/bold]")
            else:
                table_name = self.config.default_table
                console.print(f"[yellow]No relevant tables found. Using default: {table_name}[/yellow]")
        
        # Get schema for specified table
        schema = self.schema_manager.get_schema(table_name)
        
        if not schema:
            console.print(f"[bold red]Error:[/bold red] Schema not found for table '{table_name}'")
            return None
        
        # Generate SQL query with retry logic
        for attempt in range(1, self.max_retries + 1):
            # Generate SQL query
            console.print(f"[bold]Generating SQL query (attempt {attempt}/{self.max_retries})...[/bold]")
            sql_query = self.query_generator.generate_query(enhanced_query, schema, table_name)
            
            if not sql_query:
                console.print("[bold red]Error:[/bold red] Failed to generate SQL query.")
                continue
            
            # Display the generated query
            console.print(Panel(sql_query, title=f"Generated SQL Query (Attempt {attempt})", expand=False))
            
            # Execute the query
            console.print(f"[bold]Executing query (attempt {attempt}/{self.max_retries})...[/bold]")
            result = self.query_executor.execute_query(sql_query)
            
            if result is not None:
                # Query succeeded, generate summary
                console.print("[bold green]Query executed successfully![/bold green]")
                console.print("[bold]Generating summary...[/bold]")
                summary = self.summarizer.generate_summary(enhanced_query, result, original_query)
                
                return {
                    "query": sql_query,
                    "result": result,
                    "summary": summary,
                    "enhanced_query": enhanced_query,
                    "original_query": original_query,
                    "table": table_name
                }
            else:
                # Query failed, retry with a different approach
                console.print(f"[bold yellow]Query execution failed. {'Retrying...' if attempt < self.max_retries else 'All attempts failed.'}[/bold yellow]")
                time.sleep(1)  # Small delay between retries
        
        console.print("[bold red]Error:[/bold red] All query attempts failed.")
        return None

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='ClickHouse SQL Agent')
    parser.add_argument('--config', help='Path to config file', default=None)
    parser.add_argument('--query', help='Natural language query to process')
    parser.add_argument('--table', help='Table to query', default=None)
    parser.add_argument('--interactive', action='store_true', help='Run in interactive mode')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retry attempts')
    args = parser.parse_args()
    
    # Initialize agent
    agent = SQLAgent(args.config)
    agent.max_retries = args.max_retries
    
    if args.interactive:
        console.print(Panel.fit("[bold green]ClickHouse SQL Agent Interactive Mode[/bold green]"))
        console.print("Type 'exit' to quit, 'table NAME' to change table, or enter your query.")
        
        current_table = args.table
        if current_table:
            console.print(f"Current table: [bold]{current_table}[/bold]")
        else:
            console.print(f"No table specified. Will find relevant tables for each query.")
        
        while True:
            try:
                # Get user input
                user_input = console.input("[bold blue]Query>[/bold blue] ")
                
                if user_input.lower() == 'exit':
                    break
                
                # Check for table change command
                if user_input.lower().startswith('table '):
                    current_table = user_input[6:].strip()
                    console.print(f"Changed to table: [bold]{current_table}[/bold]")
                    continue
                
                # Process query
                result = agent.process_query(user_input, current_table)
                
                if result:
                    # Print summary
                    console.print(Panel(Markdown(result["summary"]), title="Summary", expand=False))
                    
                    # Print result preview
                    if isinstance(result["result"], list) and len(result["result"]) > 0:
                        preview_count = min(5, len(result["result"]))
                        
                        # Create a table for better visualization
                        result_table = Table(title=f"Results from {result['table']} ({len(result['result'])} rows)")
                        
                        # Add columns
                        columns = list(result["result"][0].keys())
                        for col in columns:
                            result_table.add_column(col)
                        
                        # Add rows
                        for row in result["result"][:preview_count]:
                            result_table.add_row(*[str(row[col]) for col in columns])
                        
                        console.print(result_table)
                        
                        if len(result["result"]) > preview_count:
                            console.print(f"[dim]Showing {preview_count} of {len(result['result'])} results[/dim]")
            
            except KeyboardInterrupt:
                console.print("\nExiting...")
                break
            except Exception as e:
                console.print(f"[bold red]Error:[/bold red] {str(e)}")
    
    elif args.query:
        # Process a single query from command line
        result = agent.process_query(args.query, args.table)
        
        if result:
            console.print(Panel(Markdown(result["summary"]), title="Summary", expand=False))
            
            # Print result preview
            if isinstance(result["result"], list) and len(result["result"]) > 0:
                preview_count = min(5, len(result["result"]))
                
                # Create a table for better visualization
                result_table = Table(title=f"Results from {result['table']} ({len(result['result'])} rows)")
                
                # Add columns
                columns = list(result["result"][0].keys())
                for col in columns:
                    result_table.add_column(col)
                
                # Add rows
                for row in result["result"][:preview_count]:
                    result_table.add_row(*[str(row[col]) for col in columns])
                
                console.print(result_table)
                
                if len(result["result"]) > preview_count:
                    console.print(f"[dim]Showing {preview_count} of {len(result['result'])} results[/dim]")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()