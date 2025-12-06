import os
import glob
import polars as pl
from dotenv import load_dotenv
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate

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

# --- PART 3: THE TOOL ---
@tool
def check_data_quality(column_name: str):
    """Checks a specific column for null values and basic stats."""
    try:
        # --- THE FIX IS HERE ---
        # The AI might send "sales" or 'sales' or sales\n. We clean it.
        column_name = column_name.strip(' "\'\n') 
        
        # Check for Nulls
        null_count = df.select(pl.col(column_name).null_count()).item()
        
        # Check for Stats
        stats_info = ""
        # We assume the column exists now that we cleaned the name
        if df[column_name].dtype in [pl.Int64, pl.Float64]:
            mean_val = df.select(pl.col(column_name).mean()).item()
            max_val = df.select(pl.col(column_name).max()).item()
            stats_info = f", Mean: {mean_val}, Max: {max_val}"
            
        return f"REPORT for '{column_name}': Found {null_count} missing values{stats_info}."
    except Exception as e:
        return f"Error checking column: {str(e)}"
@tool
def check_duplicates(input_str: str = ""):
    """
    Checks for duplicate rows in the entire dataset.
    Input is ignored, just pass an empty string.
    """
    try:
        # Count how many rows are exact duplicates of others
        # is_duplicated() returns a boolean mask, sum() counts the Trues
        duplicate_count = df.is_duplicated().sum()
        
        if duplicate_count > 0:
            return f"CRITICAL WARNING: Found {duplicate_count} duplicate rows in the dataset."
        else:
            return "Check passed: No duplicate rows found."
    except Exception as e:
        return f"Error checking duplicates: {str(e)}"

# --- PART 4: THE BRAIN (The Agent) ---
# We use 'gpt-4o' because it is smart and fast.
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# We list the tools we want to give the Agent
tools = [check_data_quality, check_duplicates]

# We load the "Personality" we wrote in instructions.txt
with open("instructions.txt", "r") as f:
    system_instructions = f.read()

# This Template defines HOW the agent thinks (ReAct: Thought -> Action -> Observation)
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

New User Input: {input}
{agent_scratchpad}
"""  # <--- CLOSE THE TRIPLE QUOTES HERE

# --- PART 5: CONNECTING IT ALL ---
prompt = PromptTemplate.from_template(template)

# Create the Agent (The Brain)
agent = create_react_agent(llm, tools, prompt)

# Create the Executor (The Body that runs the loop)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

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