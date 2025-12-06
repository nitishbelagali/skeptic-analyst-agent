import os
import glob
import polars as pl
import audit_tools
import reporting_tools
import cleaning_tools
from dotenv import load_dotenv
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory

load_dotenv()

# --- VISION: LOAD DATA (Interactive) ---
def load_data():
    print("\n🔍 SCANNING FOR DATASETS...")
    # Find all CSV files in the folder
    csv_files = glob.glob("*.csv")
    
    if not csv_files:
        raise FileNotFoundError("No CSV files found in the directory!")
    
    # Logic: List files and ask user to choose
    print(f"Found {len(csv_files)} CSV files:")
    for i, file in enumerate(csv_files, 1):
        print(f"  {i}. {file}")
    
    # Loop until we get a valid number from the user
    while True:
        try:
            selection = input(f"\nSelect a file to load (1-{len(csv_files)}): ")
            idx = int(selection) - 1 # Convert 1-based input to 0-based index
            
            if 0 <= idx < len(csv_files):
                filename = csv_files[idx]
                print(f"\n👀 SKEPTIC AGENT: Loading '{filename}'...\n")
                return pl.read_csv(filename, ignore_errors=True)
            else:
                print(f"❌ Invalid number. Please choose 1-{len(csv_files)}.")
        except ValueError:
            print("❌ Please enter a number.")

# --- INITIALIZE SESSION ---
try:
    # This now triggers the interactive menu BEFORE the agent starts
    df = load_data()
    cleaning_tools.session.load_frame(df)
except Exception as e:
    print(f"CRITICAL ERROR: {e}")
    exit()

# --- TOOLS ---
@tool
def run_deep_audit(input_str: str = ""):
    """Runs a comprehensive engineering audit checks."""
    try:
        current_df = cleaning_tools.session.current_df
        return audit_tools.run_all_checks(current_df)
    except Exception as e:
        return f"Error running deep audit: {e}"

@tool
def generate_pdf(input_str: str = ""):
    """Generates a PDF file of the last audit report."""
    return reporting_tools.generate_pdf_report()

@tool
def email_report(email_address: str):
    """Sends the audit report PDF to the given email address."""
    clean_email = email_address.strip(' "\'')
    return reporting_tools.send_email_report(clean_email)

@tool
def check_cleaning_options(input_str: str = ""):
    """Analyzes data and returns the cleaning menu."""
    report, _ = cleaning_tools.session.analyze_options()
    return report

@tool
def apply_cleaning_fix(input_str: str):
    """
    Applies a fix. Input format: "Option_ID, Strategy" (e.g., "1, median" or "0").
    """
    try:
        # 1. PARSE INPUT (Manual split because Agent sends one string)
        parts = input_str.replace(",", " ").split(maxsplit=1)
        option_id = parts[0].strip()
        strategy = parts[1].strip() if len(parts) > 1 else ""

        # 2. FUZZY MATCHING (Claude's logic, adapted safely)
        if strategy:
            s_lower = strategy.lower()
            if s_lower in ["median", "med"]: strategy = "replace with median"
            elif s_lower in ["cap", "threshold"]: strategy = "cap at threshold"
            elif s_lower in ["remove", "delete", "drop"]: strategy = "remove rows"
            elif s_lower in ["mean", "average", "avg"]: strategy = "mean"
            elif s_lower in ["zero", "0"]: strategy = "zero"
            elif s_lower in ["mode"]: strategy = "mode"
        
        # 3. APPLY FIX
        result = cleaning_tools.session.apply_fix(option_id, strategy)
        
        # 4. AUTO-SAVE & SUMMARY
        cleaning_tools.session.export_cleaned_data()
        summary = cleaning_tools.session.get_summary()
        return f"{result}\n\n(Current Data: {summary})"
        
    except Exception as e:
        return f"Error parsing input. Use format 'ID, Strategy'. Details: {e}"

@tool
def undo_last_fix(input_str: str = ""):
    """Reverts the last cleaning action."""
    result = cleaning_tools.session.undo()
    cleaning_tools.session.export_cleaned_data()
    return result

@tool
def export_cleaned_data(input_str: str = ""):
    """Saves the current state of the data to a CSV file."""
    return cleaning_tools.session.export_cleaned_data()

tools = [run_deep_audit, generate_pdf, email_report, check_cleaning_options, apply_cleaning_fix, undo_last_fix, export_cleaned_data]

# --- AGENT SETUP ---
llm = ChatOpenAI(model="gpt-4o", temperature=0)

with open("instructions.txt", "r") as f:
    system_instructions = f.read()

template = system_instructions + """

TOOLS:
------
You have access to the following tools:

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

# --- INTERACTIVE LOOP ---
print("\n🤖 SKEPTIC AGENT ONLINE. Type 'exit' to quit.\n")

while True:
    user_input = input("User (You): ")
    if user_input.lower() in ["exit", "quit", "q"]:
        break
    
    print("\n--- AGENT THINKING ---")
    try:
        response = agent_executor.invoke({"input": user_input})
        print(f"\nSkeptic Agent: {response['output']}\n")
        print("-" * 50)
    except Exception as e:
        print(f"Error: {e}")