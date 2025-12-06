import os
import glob
import polars as pl
import audit_tools
import reporting_tools
from dotenv import load_dotenv
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory

# --- PART 1: SETUP ---
# Load the secret key from .env file
load_dotenv()

# --- PART 2: THE DATA (The "Dirty" Dataset) ---
# We are creating a fake dataset with problems on purpose.
# Problem 1: A "None" (Null) value in sales.
# --- PART 2: THE DATA (Dynamic "Vision") ---
def load_data():
    # 1. Look for any CSV file in the current folder
    csv_files = glob.glob("*.csv")
    
    if not csv_files:
        raise FileNotFoundError("No CSV files found in the directory! Please drop a file like 'sales_data.csv' here.")
    
    # 2. Pick the first one we find
    filename = csv_files[0]
    print(f"\n👀 SKEPTIC AGENT: Found file '{filename}'. Loading data...\n")
    
    # 3. Read it into Polars
    # We use 'ignore_errors=True' so one bad line doesn't crash the whole agent
    return pl.read_csv(filename, ignore_errors=True)

# Load the data immediately when the app starts
try:
    df = load_data()
except Exception as e:
    print(f"CRITICAL ERROR: {e}")
    exit()

# --- PART 3: MASTER TOOL ---
@tool
def run_deep_audit(input_str: str = ""):
    """
    Runs a comprehensive engineering audit checking for:
    Schema drift, Nulls, Duplicates, Outliers, Range violations, 
    and Business Rule logic.
    """
    try:
        # We use the global 'df' we loaded earlier
        return audit_tools.run_all_checks(df)
    except Exception as e:
        return f"Error running deep audit: {e}"

@tool
def generate_pdf(input_str: str = ""):
    """Generates a PDF file of the last audit report. Input is ignored."""
    return reporting_tools.generate_pdf_report()

@tool
def email_report(email_address: str):
    """Sends the audit report PDF to the given email address."""
    # The AI sometimes sends the email with quotes, clean it up
    clean_email = email_address.strip(' "\'')
    return reporting_tools.send_email_report(clean_email)

# Update the tools list
tools = [run_deep_audit, generate_pdf, email_report]

# --- PART 4: THE BRAIN (The Agent) ---
# We use 'gpt-4o' because it is smart and fast.
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# We load the "Personality" we wrote in instructions.txt
with open("instructions.txt", "r") as f:
    system_instructions = f.read()

# This Template defines HOW the agent thinks
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

# --- PART 5: CONNECTING IT ALL ---
prompt = PromptTemplate.from_template(template)

# Create the Agent (The Brain)
agent = create_react_agent(llm, tools, prompt)

# --- NEW: Add Memory ---
# This stores the conversation history so the agent remembers what it just said
memory = ConversationBufferMemory(memory_key="chat_history")

# Create the Executor (The Body) with memory enabled
# handle_parsing_errors=True helps if the agent makes a small formatting mistake
agent_executor = AgentExecutor(
    agent=agent, 
    tools=tools, 
    verbose=True, 
    memory=memory,
    handle_parsing_errors=True
)

# --- PART 6: RUNNING IT ---
# --- PART 6: INTERACTIVE LOOP ---
print("\n🤖 SKEPTIC AGENT ONLINE. Type 'exit' to quit.\n")

while True:
    # 1. Get input from the user
    user_input = input("User (You): ")
    
    # 2. Check if the user wants to quit
    if user_input.lower() in ["exit", "quit", "q"]:
        print("Skeptic Agent: Closing audit. Goodbye.")
        break
    
    # 3. Run the Agent
    print("\n--- AGENT THINKING ---")
    try:
        response = agent_executor.invoke({"input": user_input})
        
        # 4. Extract and print just the final answer (cleaner output)
        # The 'output' key contains the Final Answer string
        print(f"\nSkeptic Agent: {response['output']}\n")
        print("-" * 50) # Just a separator line
        
    except Exception as e:
        print(f"An error occurred: {e}")