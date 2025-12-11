import streamlit as st
import polars as pl
import os
import time
import cleaning_tools
import audit_tools
import engineering_tools
import visualization_tools
import router_tools
import rag_tools
from langchain.agents import create_react_agent, AgentExecutor
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langchain_core.tools import tool
from langchain_core.prompts import PromptTemplate
from dotenv import load_dotenv
import streamlit.components.v1 as components

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Skeptic Analyst Pro",
    layout="wide",
    page_icon="🤖",
    initial_sidebar_state="expanded"
)

# --- MODERN UI CSS (Glassmorphism) ---
st.markdown("""
<style>
    /* Main Background */
    .stApp {
        background: linear-gradient(to right, #0f172a, #1e293b);
        color: #e2e8f0;
    }
    /* Cards/Containers */
    .stChatFloatingInputContainer, .stChatMessage {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
    }
    /* Headers */
    h1, h2, h3 {
        color: #38bdf8 !important;
        font-family: 'Helvetica Neue', sans-serif;
    }
    /* Buttons */
    .stButton>button {
        background: linear-gradient(90deg, #3b82f6, #8b5cf6);
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        transition: transform 0.2s;
    }
    .stButton>button:hover {
        transform: scale(1.02);
    }
</style>
""", unsafe_allow_html=True)

st.title("🤖 Skeptic Analyst Agent")
st.caption("The paranoid AI data engineer that audits, cleans, and visualizes your data.")

# Load Environment
load_dotenv()
if not os.getenv("OPENAI_API_KEY"):
    st.error("❌ OpenAI API Key not found. Please check your .env file.")
    st.stop()

# --- ARTIFACT CLEANUP ON START ---
if "app_initialized" not in st.session_state:
    for artifact in ["schema.dot", "dashboard_report.html", "warehouse.db", "dictionary.pdf"]:
        if os.path.exists(artifact):
            try:
                os.remove(artifact)
            except:
                pass
    st.session_state.app_initialized = True
    st.session_state.initial_goal = None
    st.session_state.current_mode = None

# --- HELPER: RENDER ARTIFACTS ---
def render_artifacts():
    """Checks for files on disk and renders them."""
    
    # 1. Schema Diagram (show whenever it exists)
    if os.path.exists("schema.dot"):
        try:
            with open("schema.dot", "r") as f:
                dot_code = f.read()
            if "digraph" in dot_code:
                st.caption("📐 Star Schema Entity-Relationship Diagram")
                st.graphviz_chart(dot_code)
        except Exception as e:
            st.error(f"Diagram render error: {e}")
    
    # 2. Interactive Dashboard
    if os.path.exists("dashboard_report.html"):
        file_time = os.path.getmtime("dashboard_report.html")
        st.caption(f"📊 Interactive Dashboard (Generated: {time.ctime(file_time)})")
        try:
            with open("dashboard_report.html", 'r', encoding='utf-8') as f:
                html_data = f.read()
            st.components.v1.html(html_data, height=1000, scrolling=True)
        except Exception as e:
            st.error(f"Dashboard render error: {e}")

# --- TOOL DEFINITIONS ---

@tool
def run_deep_audit(input_str: str = ""):
    """Runs comprehensive audit on current data."""
    try:
        return audit_tools.run_all_checks(cleaning_tools.session.current_df)
    except Exception as e:
        return f"❌ Audit Error: {e}"

@tool
def check_cleaning_options(input_str: str = ""):
    """Returns available cleaning options menu."""
    report, _ = cleaning_tools.session.analyze_options()
    return report

@tool
def apply_cleaning_fix(input_str: str):
    """Applies cleaning fix. Input: 'option_id strategy'."""
    try:
        clean_input = input_str.replace('"', '').replace("'", "").strip()
        parts = clean_input.replace(",", " ").split(maxsplit=1)
        option_id = parts[0].strip()
        strategy = parts[1].strip() if len(parts) > 1 else ""
        
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
def detect_data_schema(input_str: str = ""):
    """Analyzes data structure and proposes dimensional model."""
    try:
        df = cleaning_tools.session.current_df
        if df is None or df.height == 0:
            return "❌ No data loaded."
        
        # Clean up old artifacts
        for artifact in ["schema.dot", "dashboard_report.html"]:
            if os.path.exists(artifact):
                try:
                    os.remove(artifact)
                except:
                    pass
        
        plan = engineering_tools.session.detect_schema(df)
        return f"{plan}\n\n(Schema plan created. Ask user if they want to see the visual diagram.)"
        
    except Exception as e:
        return f"❌ Schema Detection Error: {e}"

@tool
def generate_schema_diagram(input_str: str = ""):
    """Generates ERD diagram and saves to schema.dot file."""
    try:
        diagram_dot = engineering_tools.session.get_schema_diagram()
        
        if not diagram_dot:
            return "❌ No schema plan found. Run detect_data_schema first."
        
        with open("schema.dot", "w") as f:
            f.write(diagram_dot)
        
        return "✅ Diagram generated and saved to disk. It will render above this message."
        
    except Exception as e:
        return f"❌ Diagram Error: {e}"

@tool
def modify_schema_plan(input_str: str):
    """Manually adjusts schema classification. Input: 'column_name, new_role'."""
    try:
        if "," not in input_str:
            return "❌ Error: Input must be 'column_name, role' (e.g., 'stream_count, dimension')"
        
        col, role = input_str.split(",", 1)
        return engineering_tools.session.modify_schema_plan(col.strip(), role.strip())
        
    except Exception as e:
        return f"❌ Modify Error: {e}"

@tool
def apply_schema_transformation(input_str: str = ""):
    """Transforms flat data into fact and dimension tables."""
    try:
        df = cleaning_tools.session.current_df
        if df is None:
            return "❌ No data to transform."
        
        # Clean up diagram (no longer needed after transformation)
        if os.path.exists("schema.dot"):
            try:
                os.remove("schema.dot")
            except:
                pass
        
        return engineering_tools.session.apply_transformation(df)
        
    except Exception as e:
        return f"❌ Transformation Error: {e}"
    
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
def load_to_warehouse(input_str: str = ""):
    """Loads transformed tables into DuckDB data warehouse."""
    try:
        return engineering_tools.session.load_to_duckdb()
    except Exception as e:
        return f"❌ Warehouse Loading Error: {e}"

@tool
def get_cleaning_history(input_str: str = ""):
    """Returns history of cleaning actions taken."""
    if hasattr(cleaning_tools.session, 'cleaning_history'):
        history = cleaning_tools.session.cleaning_history
        if history:
            return "Cleaning actions taken:\n" + "\n".join([f"- {h}" for h in history])
    return "No cleaning actions recorded yet."

@tool
def answer_with_sql(user_question: str):
    """Generates SQL query from natural language and executes it."""
    try:
        schema = engineering_tools.session.get_schema_info()
        
        if "Error" in schema or "doesn't exist" in schema or "No database" in schema:
            return "⚠️ Data warehouse not built yet. Please run 'Analyze the data' first to build the schema."
        
        llm = ChatOpenAI(model="gpt-4o", temperature=0)
        
        prompt = f"""You are a SQL expert. Given this DuckDB database schema:

{schema}

Write a SQL query to answer: "{user_question}"

Rules:
- Return ONLY the raw SQL query, no markdown formatting
- Use proper JOINs between fact and dimension tables
- Use only columns that exist in the schema
- For nested questions, use CTEs (WITH clauses) or subqueries
- Use aggregations (COUNT, SUM, AVG, MAX, MIN) where appropriate
        """
        
        response = llm.invoke(prompt)
        sql = response.content.strip().replace("```sql", "").replace("```", "").strip()
        
        result_df = engineering_tools.session.query_database(sql)
        
        # Truncate large results
        if hasattr(result_df, 'shape') and len(result_df) > 10:
            result_str = str(result_df.head(10)) + f"\n\n... ({len(result_df) - 10} more rows)"
        else:
            result_str = str(result_df)
        
        return f"Query executed successfully.\n\nSQL:\n{sql}\n\nRESULT:\n{result_str}"
        
    except Exception as e:
        return f"❌ SQL Query Error: {e}"

@tool
def create_dashboard(input_str: str = ""):
    """Generates visual dashboard with automated charts."""
    try:
        result = visualization_tools.session.generate_dashboard(context=input_str)
        
        # Verify file was created
        if os.path.exists("dashboard_report.html"):
            file_size = os.path.getsize("dashboard_report.html")
            return f"✅ Dashboard generated successfully: {result} ({file_size:,} bytes). It will render above."
        else:
            return f"⚠️ Dashboard tool completed but file not found. Result: {result}"
            
    except Exception as e:
        return f"❌ Dashboard Error: {e}"

# --- REGISTER ALL TOOLS ---
tools = [
    run_deep_audit,
    check_cleaning_options,
    preview_cleaning_fix,
    apply_cleaning_fix,
    detect_data_schema,
    generate_schema_diagram,
    modify_schema_plan,
    apply_schema_transformation,
    load_to_warehouse,
    get_cleaning_history,
    answer_with_sql,
    create_dashboard,
    rag_tools.consult_data_dictionary
]

# --- AGENT SETUP ---
if "agent_executor" not in st.session_state:
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
    
    st.session_state.agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=False,
        memory=memory,
        handle_parsing_errors=True,
        max_iterations=15
    )

# --- SIDEBAR ---
with st.sidebar:
    st.header("📂 Data Upload")
    st.divider()
    eli5_mode = st.toggle("👶 Explain Like I'm 5", value=False)
    if eli5_mode:
        st.caption("Mode: Simple analogies enabled.")
    
    # 1. CSV Uploader
    uploaded_file = st.file_uploader("1. Upload Data (CSV)", type=["csv"])
    
    if uploaded_file:
        filename = uploaded_file.name
        
        with open(filename, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        if "current_file" not in st.session_state or st.session_state.current_file != filename:
            # Clean up old artifacts
            for artifact in ["schema.dot", "dashboard_report.html", "warehouse.db"]:
                if os.path.exists(artifact):
                    try:
                        os.remove(artifact)
                    except:
                        pass
            
            try:
                df = pl.read_csv(filename, ignore_errors=True, try_parse_dates=True)
                
                cleaning_tools.session.load_frame(df, source_filename=filename)
                engineering_tools.session.reset()
                
                st.session_state.current_file = filename
                st.session_state.initial_goal = None
                st.session_state.current_mode = None
                
                st.success(f"✅ Loaded: {filename}\n📊 {df.height} rows × {df.width} columns")
                
            except Exception as e:
                st.error(f"❌ Error loading file: {e}")
    
    st.divider()
    
    # 2. PDF Uploader (RAG - Optional)
    uploaded_pdf = st.file_uploader("2. Data Dictionary (PDF - Optional)", type=["pdf"])
    
    if uploaded_pdf:
        pdf_filename = "dictionary.pdf"
        
        with open(pdf_filename, "wb") as f:
            f.write(uploaded_pdf.getbuffer())
        
        try:
            with st.spinner("🧠 Reading Dictionary..."):
                status = rag_tools.session.ingest_document(pdf_filename)
                st.success(status)
        except Exception as e:
            st.error(f"❌ RAG Error: {e}")
    
    st.divider()
    
    # Reset button
    if st.button("🔄 Reset Conversation"):
        st.session_state.messages = []
        st.session_state.initial_goal = None
        st.session_state.current_mode = None
        
        for artifact in ["schema.dot", "dashboard_report.html"]:
            if os.path.exists(artifact):
                try:
                    os.remove(artifact)
                except:
                    pass
        
        if "agent_executor" in st.session_state:
            st.session_state.agent_executor.memory.clear()
        
        st.success("Conversation reset!")
        st.rerun()
    
    # Status info
    if "current_file" in st.session_state:
        st.divider()
        st.info(f"📁 Current: {st.session_state.current_file}")
        if st.session_state.current_mode:
            mode_display = {
                "audit_only": "🔍 Audit Mode",
                "clean_data": "🔧 Cleaning Mode",
                "data_engineer": "🧠 Engineering Mode"
            }.get(st.session_state.current_mode, "")
            if mode_display:
                st.info(f"🎯 Mode: {mode_display}")

# --- MAIN CHAT INTERFACE ---
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "I am the Skeptic Analyst. Upload a CSV file, and I will audit it with extreme prejudice."
        }
    ]

if "current_mode" not in st.session_state:
    st.session_state.current_mode = None

# Render chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Render artifacts ONCE at the end
render_artifacts()

# Handle user input
if user_prompt := st.chat_input("Ask me to analyze your data..."):
    # Check if file is loaded
    if "current_file" not in st.session_state:
        st.warning("⚠️ Please upload a CSV file first!")
        st.stop()
    
    # Capture user's original goal for storytelling context
    triggers = ["analyze", "find", "get", "show", "trend", "give", "which", "what", "how"]
    if any(t in user_prompt.lower() for t in triggers):
        # Only update goal if it's a new primary question
        if not any(word in user_prompt.lower() for word in ["why", "explain", "tell me more"]):
            st.session_state.initial_goal = user_prompt
    
    # Special handling for option shortcuts
    if user_prompt.strip() == "1":
        goal = st.session_state.get("initial_goal", "General Analysis")
        user_prompt = f"Generate a detailed text data story. Explicitly answer: '{goal}'"
    
    elif user_prompt.strip() == "2":
        goal = st.session_state.get("initial_goal", "General Analysis")
        user_prompt = f"Create the dashboard and write a visual story answering: '{goal}'"
    
    # Add user message
    st.session_state.messages.append({"role": "user", "content": user_prompt})
    
    with st.chat_message("user"):
        st.markdown(user_prompt)
    
    # Generate agent response
    with st.chat_message("assistant"):
        with st.spinner("🤔 Analyzing..."):
            try:
                # Classify intent (but respect current mode)
                intent = router_tools.router.classify_intent(user_prompt)
                
                # If we're already in a mode and user gives a simple response, stay in that mode
                if st.session_state.current_mode:
                    # Check if input is a cleaning option (digit or "digit strategy")
                    first_word = user_prompt.strip().split()[0]
                    if first_word.isdigit() and st.session_state.current_mode == "clean_data":
                        intent = "clean_data"  # Stay in cleaning mode
                
                # Update current mode
                st.session_state.current_mode = intent
                
                # Add mode context
                context_prefix = {
                    "audit_only": "MODE A (AUDITOR): ",
                    "clean_data": "MODE B (SURGEON): ",
                    "data_engineer": "MODE C (ENGINEER): "
                }.get(intent, "")
                
                # Run agent
                response = st.session_state.agent_executor.invoke({
                    "input": context_prefix + user_prompt
                })
                
                output_text = response['output']
                
                # Display response
                st.markdown(output_text)
                
                # Save to history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": output_text
                })
                
                # Smart rerun: Only if artifacts were likely created
                should_rerun = False
                
                if "diagram" in output_text.lower() and ("generated" in output_text.lower() or "saved to disk" in output_text.lower()):
                    should_rerun = True
                
                if "dashboard" in output_text.lower() and "generated" in output_text.lower():
                    should_rerun = True
                
                if should_rerun:
                    time.sleep(0.5)  # Small delay to ensure file is written
                    st.rerun()
                
            except Exception as e:
                error_msg = f"❌ Error: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })