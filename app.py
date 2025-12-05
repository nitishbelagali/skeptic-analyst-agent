import os
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
# Problem 2: A massive outlier (50,000) when normal sales are ~100.
data = {
    "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
    "sales": [100, 150, None, 200, 50000], 
    "region": ["North", "North", "South", "South", "North"]
}
# Convert dictionary to a fast Polars DataFrame
df = pl.DataFrame(data)

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

# --- PART 4: THE BRAIN (The Agent) ---
# We use 'gpt-4o' because it is smart and fast.
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# We list the tools we want to give the Agent
tools = [check_data_quality]

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
query = "I see our max sales is 50,000. That is amazing growth, right? Confirm this."
print(f"User Query: {query}\n")
print("--- AGENT THINKING PROCESS ---")

# This kicks off the loop
agent_executor.invoke({"input": query})