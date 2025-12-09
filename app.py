import os
import glob
import platform
import polars as pl
import audit_tools
import reporting_tools
import cleaning_tools
import router_tools
import engineering_tools
import visualization_tools
from dotenv import load_dotenv
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory

load_dotenv()

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
        print(f"Could not auto-open file: {e}")

# --- PART 1: LOAD DATA ---
def load_data():
    """Interactive file selector with error handling"""
    print("\n🔍 SCANNING FOR DATASETS...")
    csv_files = glob.glob("*.csv")
    
    if not csv_files:
        print("❌ No CSV files found in current directory!")
        print("💡 Tip: Add a CSV file and run again.")
        return None, None
    
    print(f"Found {len(csv_files)} CSV file(s):")
    for i, file in enumerate(csv_files, 1):
        print(f"  {i}. {file}")
    
    while True:
        try:
            selection = input(f"\nSelect a file (1-{len(csv_files)}) or 'q' to quit: ")
            
            if selection.lower() in ['q', 'quit', 'exit']:
                return None, None
            
            idx = int(selection) - 1
            
            if 0 <= idx < len(csv_files):
                filename = csv_files[idx]
                print(f"\n👀 SKEPTIC AGENT: Loading '{filename}'...")
                
                try:
                    # try_parse_dates=True is crucial for the Dashboard
                    df = pl.read_csv(filename, ignore_errors=True, try_parse_dates=True)
                    print(f"✅ Loaded {df.height} rows, {df.width} columns\n")
                    return df, filename
                except Exception as e:
                    print(f"❌ Error reading file: {e}")
                    return None, None
            else:
                print(f"❌ Please enter a number between 1 and {len(csv_files)}.")
                
        except ValueError:
            print("❌ Please enter a valid number.")
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted by user.")
            return None, None

# --- PART 2: CLEANING TOOLS ---
@tool
def run_deep_audit(input_str: str = ""):
    """Runs comprehensive data audit on current dataset."""
    try:
        return audit_tools.run_all_checks(cleaning_tools.session.current_df)
    except Exception as e:
        return f"❌ Audit Error: {e}"

@tool
def generate_pdf(input_str: str = ""):
    """Generates PDF audit report."""
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
def apply_cleaning_fix(input_str: str):
    """
    Applies data cleaning fix.
    Input: "option_id strategy" (e.g., "1 median" or "0" for auto-pilot)
    """
    try:
        # Clean and parse input
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
        
        # Apply fix
        result = cleaning_tools.session.apply_fix(option_id, strategy)
        
        # Auto-save
        cleaning_tools.session.export_cleaned_data()
        
        # Try to get summary
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

# --- PART 3: ENGINEERING TOOLS ---
@tool
def detect_data_schema(input_str: str = ""):
    """Analyzes data structure and proposes dimensional model (star schema)."""
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
        return engineering_tools.session.apply_transformation(df)
    except Exception as e:
        return f"❌ Transformation Error: {e}"

@tool
def load_to_warehouse(input_str: str = ""):
    """Loads transformed tables into DuckDB data warehouse."""
    try:
        return engineering_tools.session.load_to_duckdb()
    except Exception as e:
        return f"❌ Warehouse Loading Error: {e}"

@tool
def answer_with_sql(user_question: str):
    """
    Generates SQL query from natural language and executes it.
    Example: "Which region has highest sales?"
    """
    try:
        # Get schema information
        schema_info = engineering_tools.session.get_schema_info()
        
        if "Error" in schema_info or "doesn't exist" in schema_info:
            return "❌ Data warehouse not found. Run transformation pipeline first."
        
        # Generate SQL using LLM
        sql_prompt = f"""
You are a SQL expert. Given this database schema:
{schema_info}

Write a DuckDB SQL query to answer: "{user_question}"

Rules:
- Return ONLY the raw SQL query, no markdown formatting.
- Use proper JOINs between fact and dimension tables.
- Use aggregations (COUNT, SUM, AVG) where appropriate.
        """
        
        llm_sql = ChatOpenAI(model="gpt-4o", temperature=0)
        response = llm_sql.invoke(sql_prompt)
        generated_sql = response.content.strip().replace("```sql", "").replace("```", "").strip()
        
        print(f"\n[DEBUG] Generated SQL:\n{generated_sql}\n")
        
        # Execute query
        result = engineering_tools.session.query_database(generated_sql)
        
        return f"Query executed successfully.\nSQL:\n{generated_sql}\n\nRESULT:\n{result}"
        
    except Exception as e:
        return f"❌ SQL Query Error: {e}"

@tool
def create_dashboard(input_str: str = ""):
    """Generates executive dashboard with automated charts and insights."""
    try:
        return visualization_tools.session.generate_dashboard()
    except Exception as e:
        return f"❌ Dashboard Error: {e}"

# --- PART 4: TOOL REGISTRATION ---
tools = [
    run_deep_audit, generate_pdf, email_report,
    check_cleaning_options, apply_cleaning_fix, undo_last_fix, export_cleaned_data,
    detect_data_schema, apply_schema_transformation, load_to_warehouse,
    answer_with_sql, create_dashboard
]

# --- PART 5: AGENT CONFIGURATION ---
llm = ChatOpenAI(model="gpt-4o", temperature=0)

with open("instructions.txt", "r", encoding="utf-8") as f:
    system_instructions = f.read()

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
    verbose=True,
    memory=memory,
    handle_parsing_errors=True,
    max_iterations=15  # Prevent infinite loops
)

# --- PART 6: MAIN APPLICATION LOOP ---
def main():
    """Main entry point for the Skeptic Analyst Agent"""
    print("\n" + "="*60)
    print("🤖  SKEPTIC ANALYST AGENT")
    print("    Your Paranoid Data Quality Assistant")
    print("="*60)
    
    while True:
        try:
            # Step 1: File Selection
            df, filename = load_data()
            
            if df is None:
                print("\n👋 Goodbye!")
                break
            
            # Step 2: Initialize Session
            cleaning_tools.session.load_frame(df, source_filename=filename)
            memory.clear()
            engineering_tools.session.reset()
            
            print("="*60)
            print(f"📝 Currently Working On: {filename}")
            print("="*60)
            print("\n❓ What would you like to do?")
            print("   Examples:")
            print("   • 'Just audit the data'")
            print("   • 'Clean this dataset'")
            print("   • 'Which region has highest sales?'")
            
            # Step 3: Get Initial Intent
            initial_prompt = input("\nUser (You): ").strip()
            
            if initial_prompt.lower() in ["exit", "quit", "q"]:
                print("\n👋 Goodbye!")
                break
            
            if not initial_prompt:
                print("❌ Please provide a command.")
                continue
            
            # Step 4: Route Intent
            intent = router_tools.router.classify_intent(initial_prompt)
            print(f"\n{router_tools.router.get_workflow_description(intent)}")
            print("\n--- AGENT THINKING ---\n")
            
            # Step 5: Add Context Prefix (UPDATED KEYS)
            context_prefix = {
                "audit_only": "MODE A (AUDITOR): ",
                "clean_data": "MODE B (SURGEON): ",
                "data_engineer": "MODE C (ENGINEER): "  # <--- MATCHES ROUTER_TOOLS
            }.get(intent, "")
            
            # Step 6: Execute Initial Task
            response = agent_executor.invoke({
                "input": context_prefix + initial_prompt
            })
            
            print(f"\nSkeptic Agent: {response['output']}\n")
            print("="*60)
            
            # Step 7: Auto-Generate Dashboard for Engineering Mode (UPDATED KEY)
         #   if intent == "data_engineer":  # <--- MATCHES ROUTER_TOOLS
          #      print("\n🎨 Generating Visual Dashboard...")
           #     viz_result = visualization_tools.session.generate_dashboard()
            #    print(viz_result)
                
          #      if "dashboard_report.png" in viz_result:
           #         print("📊 Opening dashboard...")
            #        open_file("dashboard_report.png")
            
            # Step 8: Conversational Loop
            while True:
                user_input = input("\nUser (You): ").strip()
                
                # Exit commands
                if user_input.lower() in ["done", "switch", "new", "restart"]:
                    print("\n🔄 Switching to new file...\n")
                    break
                
                if user_input.lower() in ["exit", "quit", "q"]:
                    print("\n👋 Goodbye!")
                    return
                
                if not user_input:
                    continue
                
                # Process follow-up query
                print("\n--- AGENT THINKING ---\n")
                
                try:
                    response = agent_executor.invoke({"input": user_input})
                    print(f"\nSkeptic Agent: {response['output']}\n")
                    print("="*60)
                except Exception as e:
                    print(f"❌ Error: {e}\n")
                    print("💡 Try rephrasing your question.\n")
        
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted by user. Goodbye!")
            break
        
        except Exception as e:
            print(f"\n❌ CRITICAL ERROR: {e}")
            print("🔄 Restarting...\n")
            continue

if __name__ == "__main__":
    main()
