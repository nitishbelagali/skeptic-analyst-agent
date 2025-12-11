import streamlit as st
import pandas as pd
import polars as pl
import os
import time
import cleaning_tools
import audit_tools
import engineering_tools
import visualization_tools
import router_tools
from langchain.agents import create_react_agent, AgentExecutor
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langchain_core.tools import tool
from langchain_core.prompts import PromptTemplate
from dotenv import load_dotenv
import streamlit.components.v1 as components

# Page Config
st.set_page_config(
    page_title="Skeptic Analyst AI", 
    layout="wide", 
    page_icon="🤖",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stChatFloatingInputContainer {bottom: 20px;}
    .block-container {padding-top: 2rem;}
    .stButton>button {width: 100%; border-radius: 5px;}
</style>
""", unsafe_allow_html=True)

st.title("🤖 Skeptic Analyst Agent")
st.caption("The paranoid AI data engineer that audits, cleans, and visualizes your data.")

# Load Environment
load_dotenv()
if not os.getenv("OPENAI_API_KEY"):
    st.error("❌ OpenAI API Key not found. Please check your .env file.")
    st.stop()

# --- 🚀 CRITICAL FIX: NUKE OLD ARTIFACTS ON SESSION START ---
if "app_initialized" not in st.session_state:
    # This block runs ONLY once when you refresh the page or open the app
    if os.path.exists("schema.dot"):
        os.remove("schema.dot")
    if os.path.exists("dashboard_report.html"):
        os.remove("dashboard_report.html")
    if os.path.exists("dashboard_report.png"):
        os.remove("dashboard_report.png")
    
    st.session_state.app_initialized = True
    st.session_state.cleaning_log = []
    st.session_state.latest_diagram = None

# --- HELPER: RENDER ARTIFACTS ---
def render_artifacts():
    """Checks for files on disk and renders them if found."""
    
    # 1. DIAGRAM
    if os.path.exists("schema.dot"):
        try:
            with open("schema.dot", "r") as f:
                dot_code = f.read()
            if "digraph" in dot_code:
                st.caption("📐 Detected Star Schema Model")
                st.graphviz_chart(dot_code)
        except Exception: pass

    # 2. DASHBOARD
    if os.path.exists("dashboard_report.html"):
        # Use file modification time as key to force reload
        file_time = os.path.getmtime("dashboard_report.html")
        st.caption(f"📊 Interactive Dashboard (Generated: {time.ctime(file_time)})")
        with open("dashboard_report.html", 'r', encoding='utf-8') as f:
            html_data = f.read()
        st.components.v1.html(html_data, height=1000, scrolling=True)

# --- TOOLS ---
@tool
def run_deep_audit(input_str: str = ""):
    """Runs audit."""
    try: return audit_tools.run_all_checks(cleaning_tools.session.current_df)
    except Exception as e: return f"Error: {e}"

@tool
def check_cleaning_options(input_str: str = ""):
    """Returns cleaning menu."""
    report, _ = cleaning_tools.session.analyze_options()
    return report

@tool
def apply_cleaning_fix(input_str: str):
    """Applies cleaning fix. Input: 'id strategy'"""
    try:
        parts = input_str.replace(",", " ").split(maxsplit=1)
        opt, strat = parts[0].strip(), parts[1].strip() if len(parts) > 1 else ""
        res = cleaning_tools.session.apply_fix(opt, strat)
        cleaning_tools.session.export_cleaned_data()
        return res
    except Exception as e: return f"Error: {e}"

@tool
def detect_data_schema(input_str: str = ""):
    """Analyzes data and SAVES schema.dot file."""
    try:
        df = cleaning_tools.session.current_df
        if df is None: return "No data."
        
        # Cleanup old dashboard to prevent confusion
        if os.path.exists("dashboard_report.html"):
            os.remove("dashboard_report.html")
        
        plan = engineering_tools.session.detect_schema(df)
        diagram_dot = engineering_tools.session.get_schema_diagram()
        
        # WRITE DIAGRAM TO DISK
        with open("schema.dot", "w") as f:
            f.write(diagram_dot)
            
        return f"{plan}\n\n(Diagram saved to disk. Ask user for confirmation.)"
    except Exception as e: return f"Error: {e}"

@tool
def apply_schema_transformation(input_str: str = ""):
    """Transforms data and CLEANS UP the diagram file."""
    # Delete the diagram file so it stops rendering
    if os.path.exists("schema.dot"):
        os.remove("schema.dot")
        
    return engineering_tools.session.apply_transformation(cleaning_tools.session.current_df)

@tool
def load_to_warehouse(input_str: str = ""):
    """Loads to DuckDB."""
    return engineering_tools.session.load_to_duckdb()

@tool
def get_cleaning_history(input_str: str = ""):
    """Returns log."""
    if hasattr(cleaning_tools.session, 'cleaning_history'):
        return str(cleaning_tools.session.cleaning_history)
    return "Log unavailable."

@tool
def answer_with_sql(user_question: str):
    """Executes SQL."""
    try:
        schema = engineering_tools.session.get_schema_info()
        llm = ChatOpenAI(model="gpt-4o", temperature=0)
        prompt = f"Target: DuckDB. Schema:\n{schema}\nRequest: '{user_question}'\nRules: Only SQL. Use valid columns."
        sql = llm.invoke(prompt).content.strip().replace("```sql", "").replace("```", "").strip()
        res = engineering_tools.session.query_database(sql)
        
        # Truncate
        if hasattr(res, 'height') and res.height > 10:
            return f"SQL:\n{sql}\nResult:\n{str(res.head(10))}\n...(truncated)"
        return f"SQL:\n{sql}\nResult:\n{str(res)}"
    except Exception as e: return f"Error: {e}"

@tool
def create_dashboard(input_str: str = ""):
    """Generates dashboard HTML."""
    try:
        res = visualization_tools.session.generate_dashboard()
        return f"Dashboard saved to {res}"
    except Exception as e: return f"Error: {e}"

tools = [run_deep_audit, check_cleaning_options, apply_cleaning_fix, detect_data_schema, 
         apply_schema_transformation, load_to_warehouse, get_cleaning_history, 
         answer_with_sql, create_dashboard]

# --- AGENT SETUP ---
if "agent_executor" not in st.session_state:
    llm = ChatOpenAI(model="gpt-4o", temperature=0)
    with open("instructions.txt", "r") as f: instructions = f.read()
    
    template = instructions + """
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
    st.session_state.agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, memory=memory, handle_parsing_errors=True, max_iterations=50)

# --- SIDEBAR ---
with st.sidebar:
    uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
    if uploaded_file:
        filename = uploaded_file.name
        with open(filename, "wb") as f: f.write(uploaded_file.getbuffer())
        
        if "current_file" not in st.session_state or st.session_state.current_file != filename:
            # Cleanup old artifacts on new file
            if os.path.exists("schema.dot"): os.remove("schema.dot")
            if os.path.exists("dashboard_report.html"): os.remove("dashboard_report.html")
            
            df = pl.read_csv(filename, ignore_errors=True, try_parse_dates=True)
            cleaning_tools.session.load_frame(df, source_filename=filename)
            engineering_tools.session.reset()
            st.session_state.current_file = filename
            st.success("✅ Loaded")

    if st.button("🔄 Reset"):
        st.session_state.messages = []
        if os.path.exists("schema.dot"): os.remove("schema.dot")
        if os.path.exists("dashboard_report.html"): os.remove("dashboard_report.html")
        if "agent_executor" in st.session_state: st.session_state.agent_executor.memory.clear()
        st.rerun()

# --- MAIN UI ---
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Ready to analyze."}]

# 1. Show History
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# 2. RENDER ARTIFACTS (Always check at bottom of history)
render_artifacts()

# 3. Handle Input
if prompt := st.chat_input("..."):
    # Special shortcuts
    if prompt == "1": prompt = "Generate a detailed text data story."
    if prompt == "2": prompt = "Create the dashboard and write a visual story."

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"): st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                intent = router_tools.router.classify_intent(prompt)
                prefix = "MODE C: " if intent == "data_engineer" else ""
                
                res = st.session_state.agent_executor.invoke({"input": prefix + prompt})
                output = res['output']
                
                st.markdown(output)
                st.session_state.messages.append({"role": "assistant", "content": output})
                
                # UNCONDITIONAL REFRESH: Ensures files created by tools appear immediately
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")