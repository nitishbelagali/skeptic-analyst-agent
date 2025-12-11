import os
import glob
import platform
import polars as pl
import time
from dotenv import load_dotenv

# Rich UI Imports
from rich.console import Console
from rich.panel import Panel
from rich.progress import track
from rich.prompt import Prompt
from rich.markdown import Markdown
from rich.status import Status

# Tool Imports
import audit_tools
import reporting_tools
import cleaning_tools
import router_tools
import engineering_tools
import visualization_tools

# LangChain Imports
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory

# --- SETUP ---
load_dotenv()
console = Console() # Initialize Rich Console

# --- UTILITY FUNCTIONS ---
def open_file(filepath):
    """Opens file with default application (cross-platform)"""
    system = platform.system()
    try:
        if system == "Darwin":  # macOS
            os.system(f"open '{filepath}'")
        elif system == "Windows":
            os.system(f'start "" "{filepath}"')
        else:  # Linux
            os.system(f"xdg-open '{filepath}'")
    except Exception as e:
        console.print(f"[red]Could not auto-open file: {e}[/red]")

# --- PART 1: LOAD DATA ---
def load_data():
    """Interactive file selector with error handling"""
    console.print("\n[bold cyan]🔍 SCANNING FOR DATASETS...[/bold cyan]")
    csv_files = glob.glob("*.csv")
    
    if not csv_files:
        console.print("[red]❌ No CSV files found in current directory![/red]")
        console.print("[yellow]💡 Tip: Add a CSV file and run again.[/yellow]")
        return None, None
    
    console.print(f"Found [bold]{len(csv_files)}[/bold] CSV file(s):")
    for i, file in enumerate(csv_files, 1):
        console.print(f"  [green]{i}. {file}[/green]")
    
    while True:
        try:
            selection = Prompt.ask("\nSelect a file number (or 'q' to quit)")
            
            if selection.lower() in ['q', 'quit', 'exit']:
                return None, None
            
            idx = int(selection) - 1
            
            if 0 <= idx < len(csv_files):
                filename = csv_files[idx]
                console.print(f"\n[dim]👀 SKEPTIC AGENT: Loading '{filename}'...[/dim]")
                
                try:
                    df = pl.read_csv(filename, ignore_errors=True, try_parse_dates=True)
                    console.print(f"[bold green]✅ Loaded {df.height} rows, {df.width} columns[/bold green]\n")
                    return df, filename
                except Exception as e:
                    console.print(f"[red]❌ Error reading file: {e}[/red]")
                    return None, None
            else:
                console.print(f"[red]❌ Please enter a number between 1 and {len(csv_files)}.[/red]")
                
        except ValueError:
            console.print("[red]❌ Please enter a valid number.[/red]")
        except KeyboardInterrupt:
            console.print("\n\n[yellow]👋 Interrupted by user.[/yellow]")
            return None, None

# --- PART 2: TOOL DEFINITIONS ---
@tool
def run_deep_audit(input_str: str = ""):
    """Runs comprehensive data audit on current dataset."""
    try:
        # Visual flair: fake progress bar for the audit
        for _ in track(range(10), description="[cyan]Auditing columns...[/cyan]"):
            time.sleep(0.05) 
        return audit_tools.run_all_checks(cleaning_tools.session.current_df)
    except Exception as e:
        return f"❌ Audit Error: {e}"

@tool
def generate_pdf(input_str: str = ""):
    """Generates PDF audit report."""
    with console.status("[bold blue]Generating PDF Report...", spinner="material"):
        return reporting_tools.generate_pdf_report()

@tool
def email_report(email_address: str):
    """Sends audit report via email (simulated)."""
    clean_email = email_address.strip(' "\'')
    return reporting_tools.send_email_report(clean_email)

@tool
def check_cleaning_options(input_str: str = ""):
    """Analyzes data and returns available cleaning options."""
    report, _ = cleaning_tools.session.analyze_options()
    return report

@tool
def preview_cleaning_fix(input_str: str):
    """Returns a DRY RUN preview of the fix to verify safety."""
    try:
        clean_input = input_str.replace('"', '').replace("'", "").strip()
        parts = clean_input.split(maxsplit=1)
        option_id = parts[0].strip()
        strategy = parts[1].strip() if len(parts) > 1 else ""
        return cleaning_tools.session.preview_fix(option_id, strategy)
    except Exception as e:
        return f"❌ Preview Error: {e}"

@tool
def apply_cleaning_fix(input_str: str):
    """Applies data cleaning fix. Input: 'option_id strategy'"""
    try:
        clean_input = input_str.replace('"', '').replace("'", "").strip()
        parts = clean_input.replace(",", " ").split(maxsplit=1)
        
        option_id = parts[0].strip()
        strategy = parts[1].strip() if len(parts) > 1 else ""
        
        # Fuzzy matching for common strategy names
        if strategy:
            s = strategy.lower()
            if s in ["median", "med"]: strategy = "replace with median"
            elif s in ["cap", "threshold"]: strategy = "cap at threshold"
            elif s in ["remove", "drop", "delete"]: strategy = "remove rows"
            elif s in ["mean", "avg", "average"]: strategy = "mean"
            elif s in ["zero", "0"]: strategy = "zero"
            elif s in ["mode"]: strategy = "mode"
        
        with console.status("[bold green]Applying fixes...", spinner="dots"):
            result = cleaning_tools.session.apply_fix(option_id, strategy)
            cleaning_tools.session.export_cleaned_data()
        
        try:
            summary = cleaning_tools.session.get_summary()
            return f"{result}\n\n📊 Current Data: {summary}"
        except:
            return result
            
    except Exception as e:
        return f"❌ Fix Error: {e}"

@tool
def undo_last_fix(input_str: str = ""):
    """Reverts the last cleaning operation."""
    result = cleaning_tools.session.undo()
    cleaning_tools.session.export_cleaned_data()
    return result

@tool
def export_cleaned_data(input_str: str = ""):
    """Exports current data state to CSV."""
    return cleaning_tools.session.export_cleaned_data()

@tool
def detect_data_schema(input_str: str = ""):
    """Analyzes data structure and proposes dimensional model."""
    try:
        df = cleaning_tools.session.current_df
        if df is None or df.height == 0:
            return "❌ No data loaded or data is empty."
        return engineering_tools.session.detect_schema(df)
    except Exception as e:
        return f"❌ Schema Detection Error: {e}"

@tool
def apply_schema_transformation(input_str: str = ""):
    """Transforms flat data into fact and dimension tables."""
    try:
        df = cleaning_tools.session.current_df
        if df is None:
            return "❌ No data to transform."
        
        with console.status("[bold purple]Transforming Data Structure...", spinner="earth"):
            return engineering_tools.session.apply_transformation(df)
    except Exception as e:
        return f"❌ Transformation Error: {e}"

@tool
def load_to_warehouse(input_str: str = ""):
    """Loads transformed tables into DuckDB data warehouse."""
    try:
        with console.status("[bold blue]Loading to Data Warehouse...", spinner="bouncingBar"):
            return engineering_tools.session.load_to_duckdb()
    except Exception as e:
        return f"❌ Warehouse Loading Error: {e}"

@tool
def get_cleaning_history(input_str: str = ""):
    """Returns the history of cleaning actions taken."""
    if hasattr(cleaning_tools.session, 'cleaning_history'):
        history = cleaning_tools.session.cleaning_history
        if history:
            return "\n".join(history)
    return "No cleaning actions recorded yet."

@tool
def answer_with_sql(user_question: str):
    """Generates SQL query from natural language and executes it."""
    try:
        schema_info = engineering_tools.session.get_schema_info()
        
        if "Error" in schema_info or "doesn't exist" in schema_info:
            return "❌ Data warehouse not found. Run transformation pipeline first."
        
        sql_prompt = f"""You are a SQL expert. Given this DuckDB database schema:

{schema_info}

Write a SQL query to answer: "{user_question}"

Rules:
- Return ONLY the raw SQL query, no markdown formatting
- Use proper JOINs between fact and dimension tables
- Use aggregations (COUNT, SUM, AVG) where appropriate
- Ensure all column references are valid
        """
        
        llm_sql = ChatOpenAI(model="gpt-4o", temperature=0)
        response = llm_sql.invoke(sql_prompt)
        generated_sql = response.content.strip().replace("```sql", "").replace("```", "").strip()
        
        # console.print(f"\n[dim][DEBUG] Generated SQL:\n{generated_sql}[/dim]\n")
        
        result = engineering_tools.session.query_database(generated_sql)
        
        return f"Query executed successfully.\nSQL:\n{generated_sql}\n\nRESULT:\n{result}"
        
    except Exception as e:
        return f"❌ SQL Query Error: {e}"

@tool
def create_dashboard(input_str: str = ""):
    """Generates executive dashboard with automated charts."""
    try:
        with console.status("[bold yellow]Rendering Visualization...", spinner="bouncingBar"):
            result = visualization_tools.session.generate_dashboard(context=input_str)
        
        if "dashboard_report.html" in str(result):
            open_file("dashboard_report.html")
            
        return f"✅ Dashboard generated: {result}"
    except Exception as e:
        return f"❌ Dashboard Error: {e}"

# --- PART 3: TOOL REGISTRATION ---
tools = [
    run_deep_audit,
    generate_pdf,
    email_report,
    check_cleaning_options,
    preview_cleaning_fix, # New v2.0 Tool
    apply_cleaning_fix,
    undo_last_fix,
    export_cleaned_data,
    detect_data_schema,
    apply_schema_transformation,
    load_to_warehouse,
    get_cleaning_history,
    answer_with_sql,
    create_dashboard
]

# --- PART 4: AGENT CONFIGURATION ---
def setup_agent(eli5_mode=False):
    llm = ChatOpenAI(model="gpt-4o", temperature=0)

    with open("instructions.txt", "r", encoding="utf-8") as f:
        system_instructions = f.read()

    # ELI5 Logic Injection
    if eli5_mode:
        system_instructions += "\n\nCRITICAL STYLE OVERRIDE: EXPLAIN EVERYTHING LIKE I AM 5 YEARS OLD. USE ANALOGIES (Legos, Pizza, Toys)."

    template = system_instructions + """

TOOLS:
------
{tools}

To use a tool, please use the following format:

Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action

When you have a response to say to the Human, or if you do not need to use a tool, you MUST use the format:

Thought: Do I need to use a tool? No
Final Answer: [your response here]

Begin!

Previous Conversation:
{chat_history}

New User Input: {input}
{agent_scratchpad}
"""

    prompt = PromptTemplate.from_template(template)
    agent = create_react_agent(llm, tools, prompt)
    memory = ConversationBufferMemory(memory_key="chat_history")

    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=False, # Set to False so we can control output with Rich
        memory=memory,
        handle_parsing_errors=True,
        max_iterations=15
    )
    return agent_executor, memory

# --- PART 5: MAIN APPLICATION LOOP ---
def main():
    """Main entry point for the Skeptic Analyst Agent"""
    console.clear()
    console.print(Panel.fit(
        "[bold cyan]🤖 SKEPTIC ANALYST AGENT v2.0[/bold cyan]\n[dim]Your Paranoid Data Quality Assistant[/dim]",
        border_style="cyan"
    ))
    
    while True:
        try:
            # Step 1: File Selection
            df, filename = load_data()
            
            if df is None:
                console.print("\n[yellow]👋 Goodbye![/yellow]")
                break
            
            # Step 2: Initialize Session
            cleaning_tools.session.load_frame(df, source_filename=filename)
            engineering_tools.session.reset()
            
            # ELI5 Toggle for CLI
            eli5_input = Prompt.ask("\n👶 Enable 'Explain Like I'm 5' Mode?", choices=["y", "n"], default="n")
            eli5_mode = (eli5_input == "y")
            
            agent_executor, memory = setup_agent(eli5_mode)

            console.print("="*60)
            console.print(f"📝 Currently Working On: [bold]{filename}[/bold]")
            if eli5_mode: console.print("[magenta]👶 ELI5 Mode: ENABLED[/magenta]")
            console.print("="*60)
            
            console.print("\n❓ What would you like to do?")
            console.print("   Examples:")
            console.print("   • 'Just audit the data'")
            console.print("   • 'Clean this dataset'")
            console.print("   • 'Which region has highest sales?'")
            
            # Step 3: Get Initial Intent
            initial_prompt = console.input("\n[bold yellow]User (You) > [/bold yellow]").strip()
            
            if initial_prompt.lower() in ["exit", "quit", "q"]:
                console.print("\n[yellow]👋 Goodbye![/yellow]")
                break
            
            if not initial_prompt:
                console.print("[red]❌ Please provide a command.[/red]")
                continue
            
            # Step 4: Route Intent
            intent = router_tools.router.classify_intent(initial_prompt)
            console.print(f"\n[dim]{router_tools.router.get_workflow_description(intent)}[/dim]")
            
            # Step 5: Add Context Prefix
            context_prefix = {
                "audit_only": "MODE A (AUDITOR): ",
                "clean_data": "MODE B (SURGEON): ",
                "data_engineer": "MODE C (ENGINEER): "
            }.get(intent, "")
            
            # Step 6: Execute Initial Task
            with console.status("Thinking...", spinner="dots"):
                response = agent_executor.invoke({
                    "input": context_prefix + initial_prompt
                })
            
            # Use Markdown for pretty printing agent response
            console.print(Panel(Markdown(response['output']), title="Skeptic Agent", border_style="green"))
            
            # Step 7: Conversational Loop
            while True:
                user_input = console.input("\n[bold yellow]User (You) > [/bold yellow]").strip()
                
                # Exit commands
                if user_input.lower() in ["done", "switch", "new", "restart"]:
                    console.print("\n[bold cyan]🔄 Switching to new file...[/bold cyan]\n")
                    break
                
                if user_input.lower() in ["exit", "quit", "q"]:
                    console.print("\n[yellow]👋 Goodbye![/yellow]")
                    return
                
                if not user_input:
                    continue
                
                try:
                    with console.status("Thinking...", spinner="dots"):
                        response = agent_executor.invoke({"input": user_input})
                    
                    console.print(Panel(Markdown(response['output']), title="Skeptic Agent", border_style="green"))
                    
                except Exception as e:
                    console.print(f"[red]❌ Error: {e}[/red]\n")
                    console.print("[dim]💡 Try rephrasing your question.[/dim]\n")
        
        except KeyboardInterrupt:
            console.print("\n\n[yellow]👋 Interrupted by user. Goodbye![/yellow]")
            break
        
        except Exception as e:
            console.print(f"\n[red]❌ CRITICAL ERROR: {e}[/red]")
            console.print("🔄 Restarting...\n")
            continue

if __name__ == "__main__":
    main()