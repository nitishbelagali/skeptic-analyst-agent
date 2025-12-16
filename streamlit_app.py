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
import reporting_tools
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

# --- MODERN UI CSS ---
st.markdown("""
<style>
    .stApp { background: linear-gradient(to right, #0f172a, #1e293b); color: #e2e8f0; }
    .stChatFloatingInputContainer, .stChatMessage {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
    }
    h1, h2, h3 { color: #38bdf8 !important; }
    .stButton>button {
        background: linear-gradient(90deg, #3b82f6, #8b5cf6);
        color: white; border: none; padding: 0.5rem 1rem; border-radius: 8px;
    }
    .stButton>button:hover { transform: scale(1.02); }
    .stDownloadButton>button { background: linear-gradient(90deg, #10b981, #059669); color: white; }
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
all_artifacts = [
    "schema.dot", "schema.png", "dashboard_report.html", "warehouse.db", "dictionary.pdf",
    "Audit_Report.pdf", "Deep_Dive_Analysis.pdf", "Dashboard_Report.pdf"
]

if "generated_artifacts" not in st.session_state:
    st.session_state.generated_artifacts = set()

# Clean up stale files
for artifact in all_artifacts:
    if os.path.exists(artifact) and artifact not in st.session_state.generated_artifacts:
        try: os.remove(artifact)
        except: pass

if "app_initialized" not in st.session_state:
    st.session_state.app_initialized = True
    st.session_state.initial_goal = None
    st.session_state.current_mode = None

# --- TOOL DEFINITIONS ---

@tool
def run_deep_audit(input_str: str = ""):
    """Runs comprehensive audit on current data."""
    try: return audit_tools.run_all_checks(cleaning_tools.session.current_df)
    except Exception as e: return f"❌ Audit Error: {e}"

@tool
def generate_pdf(input_str: str = ""):
    """Generates a PDF audit report of the findings."""
    return reporting_tools.generate_pdf_report()

@tool
def generate_analysis_pdf(analysis_text: str):
    """Generates a PDF report containing the deep dive text analysis."""
    return reporting_tools.generate_analysis_pdf(analysis_text)

@tool
def generate_dashboard_pdf(input_str: str = ""):
    """Generates a PDF snapshot of the current dashboard."""
    return reporting_tools.generate_dashboard_pdf()

@tool
def check_cleaning_options(input_str: str = ""):
    """Returns the menu of available cleaning options."""
    report, _ = cleaning_tools.session.analyze_options()
    return report

@tool
def preview_cleaning_fix(input_str: str):
    """Returns a dry-run preview of what a cleaning action will do."""
    try:
        clean_input = input_str.replace('"', '').replace("'", "").strip()
        return cleaning_tools.session.preview_fix(clean_input)
    except Exception as e: return f"❌ Preview Error: {e}"

@tool
def apply_cleaning_fix(input_str: str):
    """Applies cleaning fix. Input: '0' or '1, 2, 3' or '1 remove'."""
    try:
        clean_input = input_str.replace('"', '').replace("'", "").strip()
        result = cleaning_tools.session.apply_fix(clean_input)
        cleaning_tools.session.export_cleaned_data()
        try:
            summary = cleaning_tools.session.get_summary()
            return f"{result}\n\n📊 Current Data: {summary}"
        except: return result
    except Exception as e: return f"❌ Fix Error: {e}"

@tool
def export_cleaned_data(input_str: str = ""):
    """Exports the current state of the data to a CSV file."""
    return cleaning_tools.session.export_cleaned_data()

@tool
def detect_data_schema(input_str: str = ""):
    """Analyzes data structure and proposes a dimensional model (Star Schema)."""
    try:
        df = cleaning_tools.session.current_df
        if df is None or df.height == 0: return "❌ No data loaded."
        for artifact in ["schema.dot", "schema.png", "dashboard_report.html"]:
            if os.path.exists(artifact):
                try: os.remove(artifact)
                except: pass
        plan = engineering_tools.session.detect_schema(df)
        return f"{plan}\n\n(Schema plan created. Ask user if they want to see the visual diagram.)"
    except Exception as e: return f"❌ Schema Detection Error: {e}"

@tool
def generate_schema_diagram(input_str: str = ""):
    """Generates and saves an ER diagram of the proposed schema."""
    try:
        diagram_dot = engineering_tools.session.get_schema_diagram()
        if not diagram_dot: return "❌ No schema plan found."
        
        with open("schema.dot", "w") as f: 
            f.write(diagram_dot)
        
        try:
            import subprocess
            subprocess.run(["dot", "-Tpng", "schema.dot", "-o", "schema.png"], 
                          check=True, capture_output=True, timeout=10)
            return "✅ Diagram generated and saved."
        except: 
            return "✅ Diagram generated (schema.dot). Install graphviz for PNG rendering."
            
    except Exception as e: return f"❌ Diagram Error: {e}"

@tool
def modify_schema_plan(input_str: str):
    """Manually adjusts schema classification. Input format: 'column_name, new_role'."""
    try:
        if "," not in input_str: return "❌ Error: Input must be 'column_name, role'"
        col, role = input_str.split(",", 1)
        df = cleaning_tools.session.current_df
        if df is None: return "❌ No data loaded."
        if col.strip() not in df.columns: return f"❌ Column '{col.strip()}' not found."
        
        result = engineering_tools.session.modify_schema_plan(col, role, df_context=df)
        if "✅" in result:
            try:
                diagram_dot = engineering_tools.session.get_schema_diagram()
                with open("schema.dot", "w") as f: f.write(diagram_dot)
                import subprocess
                subprocess.run(["dot", "-Tpng", "schema.dot", "-o", "schema.png"], check=True, capture_output=True, timeout=10)
            except: pass
        return result
    except Exception as e: return f"❌ Modify Error: {e}"

@tool
def apply_schema_transformation(input_str: str = ""):
    """Transforms the flat dataframe into fact and dimension tables based on the plan."""
    try:
        df = cleaning_tools.session.current_df
        if df is None: return "❌ No data."
        return engineering_tools.session.apply_transformation(df)
    except Exception as e: return f"❌ Transformation Error: {e}"

@tool
def load_to_warehouse(input_str: str = ""):
    """Loads transformed tables into a local DuckDB data warehouse."""
    try: 
        result = engineering_tools.session.load_to_duckdb()
        # Force diagram generation on load
        try:
            diagram_dot = engineering_tools.session.get_schema_diagram()
            if diagram_dot:
                with open("schema.dot", "w") as f: f.write(diagram_dot)
                import subprocess
                subprocess.run(["dot", "-Tpng", "schema.dot", "-o", "schema.png"], check=True, capture_output=True, timeout=10)
        except: pass
        return result
    except Exception as e: return f"❌ Warehouse Loading Error: {e}"

@tool
def get_cleaning_history(input_str: str = ""):
    """Retrieves the log of all cleaning actions performed."""
    if hasattr(cleaning_tools.session, 'cleaning_history'):
        return "Cleaning History:\n" + "\n".join([f"- {h}" for h in cleaning_tools.session.cleaning_history])
    return "No cleaning actions recorded."

@tool
def answer_with_sql(user_question: str):
    """Generates and executes a SQL query to answer a natural language question about the data."""
    try:
        schema = engineering_tools.session.get_schema_info()
        if "Error" in schema or "No database" in schema: return "⚠️ Warehouse not built."
        
        # Use mini model to handle rate limits better
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        prompt = f"Schema:\n{schema}\nWrite SQL query for: '{user_question}'. Return ONLY raw SQL."
        response = llm.invoke(prompt)
        sql = response.content.strip().replace("```sql", "").replace("```", "").strip()
        result_df = engineering_tools.session.query_database(sql)
        
        if hasattr(result_df, 'shape') and len(result_df) > 10:
            result_str = str(result_df.head(10)) + f"\n\n... ({len(result_df) - 10} more rows)"
        else: result_str = str(result_df)
        return f"SQL:\n{sql}\n\nRESULT:\n{result_str}"
    except Exception as e: return f"❌ SQL Query Error: {e}"

@tool
def create_dashboard(input_str: str = ""):
    """Generates an interactive HTML dashboard with automated charts."""
    try:
        result = visualization_tools.visualize_data_tool(input_str)
        if os.path.exists("dashboard_report.html"): return result
        return f"⚠️ Dashboard tool completed but file not found. Result: {result}"
    except Exception as e: return f"❌ Dashboard Error: {e}"

tools = [
    run_deep_audit, generate_pdf, generate_analysis_pdf, generate_dashboard_pdf,
    check_cleaning_options, preview_cleaning_fix, apply_cleaning_fix, export_cleaned_data,
    detect_data_schema, generate_schema_diagram, modify_schema_plan, apply_schema_transformation,
    load_to_warehouse, get_cleaning_history, answer_with_sql, create_dashboard,
    rag_tools.consult_data_dictionary
]

# --- AGENT SETUP ---
if "agent_executor" not in st.session_state:
    # Switched to GPT-4o with auto-retry
    llm = ChatOpenAI(model="gpt-4o", temperature=0, max_retries=5)
    
    with open("instructions.txt", "r", encoding="utf-8") as f:
        system_instructions = f.read()
    
    # STRICT PROMPT TEMPLATE (Fixed: Added {{tool_names}})
    template = system_instructions + """
TOOLS:
------
You have access to the following tools:
{tools}

**FORMATTING INSTRUCTIONS (YOU MUST FOLLOW THESE EXACTLY):**
1. Each step MUST include 'Thought:', 'Action:', and 'Action Input:'.
2. **Action Input:** If the tool does not need specific input, you MUST write "Action Input: none" or "Action Input: \"\"". Do not leave it empty.
3. Do not output the 'Observation:' prefix yourself; the system will provide it.

**RESPONSE FORMAT:**
Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: [input string]
Observation: [system provides this]

... (repeat as needed) ...

Thought: Do I need to use a tool? No
Final Answer: [your response to the user]

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
        verbose=True,
        memory=memory,
        handle_parsing_errors=True,
        max_iterations=30
    )

# --- SIDEBAR ---
with st.sidebar:
    st.header("📂 Data Upload")
    uploaded_file = st.file_uploader("1. Upload Data (CSV)", type=["csv"])
    if uploaded_file:
        filename = uploaded_file.name
        with open(filename, "wb") as f: f.write(uploaded_file.getbuffer())
        
        if "current_file" not in st.session_state or st.session_state.current_file != filename:
            for artifact in all_artifacts:
                if os.path.exists(artifact) and artifact not in st.session_state.generated_artifacts:
                    try: os.remove(artifact)
                    except: pass
            try:
                df = pl.read_csv(filename, ignore_errors=True, try_parse_dates=True)
                cleaning_tools.session.load_frame(df, source_filename=filename)
                engineering_tools.session.reset()
                st.session_state.current_file = filename
                st.session_state.initial_goal = None
                st.session_state.current_mode = None
                st.session_state.generated_artifacts = set()
                st.success(f"✅ Loaded: {filename}")
            except Exception as e: st.error(f"Error: {e}")

    uploaded_pdf = st.file_uploader("2. Data Dictionary (Optional PDF)", type=["pdf"])
    if uploaded_pdf:
        with open("dictionary.pdf", "wb") as f: f.write(uploaded_pdf.getbuffer())
        try: st.success(rag_tools.session.ingest_document("dictionary.pdf"))
        except Exception as e: st.error(f"RAG Error: {e}")

    st.divider()
    st.subheader("📥 Downloads")
    
    def is_fresh(filename):
        return (os.path.exists(filename) and "generated_artifacts" in st.session_state and 
                filename in st.session_state.generated_artifacts)

    if is_fresh("Audit_Report.pdf"):
        with open("Audit_Report.pdf", "rb") as f: st.download_button("📄 Audit Report", f, "Audit_Report.pdf")
    if is_fresh("Deep_Dive_Analysis.pdf"):
        with open("Deep_Dive_Analysis.pdf", "rb") as f: st.download_button("📊 Analysis Report", f, "Deep_Dive_Analysis.pdf")
    if is_fresh("Dashboard_Report.pdf"):
        with open("Dashboard_Report.pdf", "rb") as f: st.download_button("📈 Dashboard PDF", f, "Dashboard_Report.pdf")
    if os.path.exists("dashboard_report.html"):
        try:
            with open("dashboard_report.html", "rb") as f: st.download_button("🌐 Dashboard HTML", f, "dashboard_report.html")
        except: pass
    if "current_file" in st.session_state:
        clean_filename = f"clean_{st.session_state.current_file}"
        if os.path.exists(clean_filename):
            with open(clean_filename, "rb") as f: st.download_button("🧹 Cleaned CSV", f, clean_filename)

    st.divider()
    if st.button("🔄 Reset Conversation"):
        st.session_state.messages = []
        st.session_state.initial_goal = None
        st.session_state.current_mode = None
        st.session_state.generated_artifacts = set()
        for artifact in all_artifacts:
            if os.path.exists(artifact):
                try: os.remove(artifact)
                except: pass
        if "agent_executor" in st.session_state: st.session_state.agent_executor.memory.clear()
        st.success("Reset!")
        st.rerun()

    if "current_file" in st.session_state:
        st.divider()
        st.info(f"📁 {st.session_state.current_file}")
        if st.session_state.current_mode: st.info(f"🎯 {st.session_state.current_mode}")

# --- MAIN CHAT ---
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "I am the Skeptic Analyst. Upload a CSV file, and I will audit it with extreme prejudice."}]

# --- UPDATED CHAT RENDER LOOP (INLINE ARTIFACTS) ---
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        
        # Render Attached Artifacts (INLINE)
        if "artifact" in msg:
            artifact_type = msg["artifact"]
            
            # Case 1: Schema Diagram (Resized to 600px width)
            if artifact_type == "schema":
                if os.path.exists("schema.png"):
                    st.image("schema.png", width=600)
                elif os.path.exists("schema.dot"):
                    with open("schema.dot", "r") as f: st.graphviz_chart(f.read())
            
            # Case 2: Dashboard (Resized to 600px height)
            elif artifact_type == "dashboard":
                if os.path.exists("dashboard_report.html"):
                    with open("dashboard_report.html", 'r', encoding='utf-8') as f:
                        st.components.v1.html(f.read(), height=600, scrolling=True)

if user_prompt := st.chat_input("Ask me to analyze your data..."):
    if "current_file" not in st.session_state:
        st.warning("⚠️ Upload a CSV file first!")
        st.stop()
    
    ignore_list = ["yes", "no", "0", "1", "2", "3", "proceed", "done", "finish", "why", "confirm"]
    if not any(word == user_prompt.lower().strip() for word in ignore_list):
        st.session_state.initial_goal = user_prompt
    if st.session_state.initial_goal is None: st.session_state.initial_goal = "General Data Analysis"
    
    original_prompt = user_prompt
    if user_prompt.strip() == "1": user_prompt = f"Generate a detailed text data story. Answer: '{st.session_state.initial_goal}'"
    elif user_prompt.strip() == "2": user_prompt = f"Create dashboard and visual story. Answer: '{st.session_state.initial_goal}'"
    elif user_prompt.strip() == "3": user_prompt = "I want to run a custom SQL query."

    st.session_state.messages.append({"role": "user", "content": original_prompt})
    with st.chat_message("user"): st.markdown(original_prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("🤔 Analyzing..."):
            try:
                intent = router_tools.router.classify_intent(user_prompt)
                if st.session_state.current_mode == "data_engineer" and user_prompt.strip() in ["1", "2", "3"]: intent = "data_engineer"
                elif st.session_state.current_mode == "clean_data" and user_prompt.strip().split()[0].isdigit(): intent = "clean_data"
                
                st.session_state.current_mode = intent
                context_prefix = {"audit_only": "MODE A: ", "clean_data": "MODE B: ", "data_engineer": "MODE C: "}.get(intent, "")
                
                response = st.session_state.agent_executor.invoke({"input": context_prefix + user_prompt})
                output_text = response['output']
                
                # --- CAPTURE NEW ARTIFACTS ---
                current_artifact = None
                
                # 1. Register PDFs
                if "Audit_Report.pdf" in output_text: st.session_state.generated_artifacts.add("Audit_Report.pdf")
                if "Deep_Dive_Analysis.pdf" in output_text: st.session_state.generated_artifacts.add("Deep_Dive_Analysis.pdf")
                if "Dashboard_Report.pdf" in output_text: st.session_state.generated_artifacts.add("Dashboard_Report.pdf")

                # 2. Capture Diagrams
                if any(x in output_text.lower() for x in ["diagram", "schema", "warehouse built", "warehouse created"]):
                    st.session_state.generated_artifacts.add("schema.png")
                    st.session_state.generated_artifacts.add("schema.dot")
                    st.session_state.generated_artifacts.add("warehouse.db")
                    current_artifact = "schema"

                # 3. Capture Dashboard
                if any(x in output_text.lower() for x in ["dashboard", "visual story"]):
                    st.session_state.generated_artifacts.add("dashboard_report.html")
                    current_artifact = "dashboard"

                st.markdown(output_text)
                
                # SAVE MESSAGE WITH ARTIFACT TAG
                msg_data = {"role": "assistant", "content": output_text}
                if current_artifact:
                    msg_data["artifact"] = current_artifact
                st.session_state.messages.append(msg_data)
                
                # Rerun if something visual was likely created
                if current_artifact:
                    time.sleep(1.0) 
                    st.rerun()
            except Exception as e:
                st.error(f"❌ Error: {e}")