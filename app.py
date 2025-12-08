import os
import glob
import polars as pl
import audit_tools
import reporting_tools
import cleaning_tools
import router_tools
import engineering_tools
from dotenv import load_dotenv
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory

load_dotenv()

# --- PART 1: LOAD DATA ---
def load_data():
    print("\n🔍 SCANNING FOR DATASETS...")
    csv_files = glob.glob("*.csv")
    if not csv_files: raise FileNotFoundError("No CSV files found!")
    
    print(f"Found {len(csv_files)} CSV files:")
    for i, file in enumerate(csv_files, 1): print(f"  {i}. {file}")
        
    while True:
        try:
            selection = input(f"\nSelect a file (1-{len(csv_files)}) or 'q' to quit: ")
            if selection.lower() in ['q', 'quit']: return None, None
            idx = int(selection) - 1
            if 0 <= idx < len(csv_files):
                filename = csv_files[idx]
                print(f"\n👀 SKEPTIC AGENT: Loading '{filename}'...\n")
                return pl.read_csv(filename, ignore_errors=True), filename
            print(f"❌ Invalid number.")
        except ValueError: print("❌ Enter a number.")

# --- PART 2: EXISTING TOOLS (Audit/Clean) ---
@tool
def run_deep_audit(input_str: str = ""):
    """Runs audit on current data."""
    try: return audit_tools.run_all_checks(cleaning_tools.session.current_df)
    except Exception as e: return f"Error: {e}"

@tool
def generate_pdf(input_str: str = ""):
    """Generates PDF report."""
    return reporting_tools.generate_pdf_report()

@tool
def email_report(email_address: str):
    """Sends email report."""
    return reporting_tools.send_email_report(email_address.strip(' "\''))

@tool
def check_cleaning_options(input_str: str = ""):
    """Returns cleaning menu."""
    report, _ = cleaning_tools.session.analyze_options()
    return report

@tool
def apply_cleaning_fix(input_str: str):
    """
    Applies a fix. Input format: "Option_ID, Strategy" (e.g., "1, median" or "0").
    """
    try:
        # FIX: Strip quotes to handle inputs like '"0"' or "'1', 'mean'"
        clean_input = input_str.replace('"', '').replace("'", "")
        
        # Robust Parsing
        parts = clean_input.replace(",", " ").split(maxsplit=1)
        option_id = parts[0].strip()
        strategy = parts[1].strip() if len(parts) > 1 else ""

        # Fuzzy Matching Logic
        if strategy:
            s_lower = strategy.lower()
            if s_lower in ["median", "med"]: strategy = "replace with median"
            elif s_lower in ["cap", "threshold"]: strategy = "cap at threshold"
            elif s_lower in ["remove", "delete", "drop"]: strategy = "remove rows"
            elif s_lower in ["mean", "average", "avg"]: strategy = "mean"
            elif s_lower in ["zero", "0"]: strategy = "zero"
            elif s_lower in ["mode"]: strategy = "mode"
        
        result = cleaning_tools.session.apply_fix(option_id, strategy)
        
        # Auto-Save & Summary
        cleaning_tools.session.export_cleaned_data()
        try:
            summary = cleaning_tools.session.get_summary()
            return f"{result}\n\n(Current Data: {summary})"
        except Exception:
            return result
        
    except Exception as e:
        return f"Error parsing input. Use format 'ID, Strategy'. Details: {e}"

@tool
def undo_last_fix(input_str: str = ""):
    """Undos last action."""
    res = cleaning_tools.session.undo()
    cleaning_tools.session.export_cleaned_data()
    return res

@tool
def export_cleaned_data(input_str: str = ""):
    """Saves data to CSV."""
    return cleaning_tools.session.export_cleaned_data()

# --- PART 3: NEW ENGINEERING TOOLS (Mode C) ---
@tool
def detect_data_schema(input_str: str = ""):
    """Proposes a Dimensional Model (Star Schema) based on current data."""
    try:
        df = cleaning_tools.session.current_df
        return engineering_tools.session.detect_schema(df)
    except Exception as e: return f"Error detecting schema: {e}"

@tool
def apply_schema_transformation(input_str: str = ""):
    """Transforms flat data into Fact and Dimension tables."""
    try:
        df = cleaning_tools.session.current_df
        return engineering_tools.session.apply_transformation(df)
    except Exception as e: return f"Error transforming: {e}"

@tool
def load_to_warehouse(input_str: str = ""):
    """Loads transformed tables into DuckDB warehouse."""
    return engineering_tools.session.load_to_duckdb()

@tool
def answer_with_sql(user_question: str):
    """
    Generates and executes SQL to answer a user question.
    Input: The natural language question (e.g. "Which city has max sales?")
    """
    try:
        # 1. Get Schema
        schema_info = engineering_tools.session.get_schema_info()
        if "Error" in schema_info or "doesn't exist" in schema_info:
            return "❌ Error: Data warehouse not found. Run 'load_to_warehouse' first."

        # 2. Ask LLM for SQL
        sql_prompt = f"""
        You are a SQL Expert. Given this database schema:
        {schema_info}
        
        Write a DuckDB SQL query to answer: "{user_question}"
        
        Rules:
        - Return ONLY the raw SQL query. No markdown, no explanation.
        - Use fully qualified names if needed.
        - DuckDB syntax.
        """
        
        llm_sql = ChatOpenAI(model="gpt-4o", temperature=0)
        generated_sql = llm_sql.invoke(sql_prompt).content.strip().replace("```sql", "").replace("```", "")
        
        print(f"\n[DEBUG] Generated SQL: {generated_sql}")
        
        # 3. Execute SQL
        result = engineering_tools.session.query_database(generated_sql)
        
        return f"Query executed successfully.\nSQL: {generated_sql}\n\nRESULT:\n{result}"
        
    except Exception as e:
        return f"Error answering question: {e}"

# Register all tools
tools = [
    run_deep_audit, generate_pdf, email_report, 
    check_cleaning_options, apply_cleaning_fix, undo_last_fix, export_cleaned_data,
    detect_data_schema, apply_schema_transformation, load_to_warehouse, answer_with_sql
]

# --- PART 4: AGENT CONFIG ---
llm = ChatOpenAI(model="gpt-4o", temperature=0)
with open("instructions.txt", "r") as f: system_instructions = f.read()

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
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, memory=memory, handle_parsing_errors=True)

# --- PART 5: MAIN LOOP ---
def main():
    print("\n🤖 SKEPTIC AGENT ONLINE.")
    
    while True:
        try:
            # 1. File Selection
            df, filename = load_data()
            if df is None: break
            
            cleaning_tools.session.load_frame(df, source_filename=filename)
            memory.clear()
            engineering_tools.session.reset() # Clear old DB state
            
            print(f"-" * 50)
            print(f"📝 Loaded: {filename}")
            print("\n❓ How can I help? (e.g., 'Just audit', 'Clean it', 'Find max sales')")
            
            # 2. Router / First Prompt
            initial_prompt = input("\nUser (You): ")
            if initial_prompt.lower() in ["exit", "quit", "q"]: break
            
            intent = router_tools.router.classify_intent(initial_prompt)
            print(f"\n{router_tools.router.get_workflow_description(intent)}\n")
            
            # 3. Inject Context & Run
            # We prefix the user's input with the Mode Context so the LLM follows instructions.txt
            context_prefix = ""
            if intent == "audit_only": context_prefix = "MODE A (AUDITOR): "
            elif intent == "clean_data": context_prefix = "MODE B (SURGEON): "
            elif intent == "data_engineer": context_prefix = "MODE C (ENGINEER): "
            
            response = agent_executor.invoke({"input": context_prefix + initial_prompt})
            print(f"\nSkeptic Agent: {response['output']}\n")

            # 4. Chat Loop
            while True:
                user_input = input("User (You): ")
                if user_input.lower() in ["done", "switch", "new"]: 
                    print(f"\n🔄 Restarting...\n"); break
                if user_input.lower() in ["exit", "quit", "q"]: return
                
                print("\n--- AGENT THINKING ---")
                try:
                    response = agent_executor.invoke({"input": user_input})
                    print(f"\nSkeptic Agent: {response['output']}\n")
                    print("-" * 50)
                except Exception as e: print(f"Error: {e}")

        except Exception as e:
            print(f"CRITICAL ERROR: {e}")
            break

if __name__ == "__main__":
    main()