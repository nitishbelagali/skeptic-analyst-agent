# 🕵️‍♂️ The Skeptic Analyst Agent

> *"I don't trust your data until I verify it."*

The **Skeptic Analyst** is an autonomous AI agent designed to audit data quality with extreme prejudice. Unlike standard assistants that blindly accept data inputs, this agent assumes all data is "dirty," performs rigorous engineering checks, and refuses to sign off until the data is proven clean.

## 🚀 Key Features

* **Autonomic Vision:** Automatically detects and reads CSV files from the local directory.
* **Paranoid Audit Suite:** Runs 30+ engineering checks including:
    * Null Explosion & Schema Drift
    * Duplicate Detection
    * Outlier Analysis & Range Violations
    * Business Rule Logic (e.g., Invalid Regions)
* **ReAct Cognitive Architecture:** Uses a "Reason + Act" loop to form hypotheses about data errors before verifying them.
* **Professional Reporting:** Generates downloadable **PDF Audit Reports** and simulates email delivery to stakeholders.
* **Memory:** Remembers context across the chat session to handle multi-step workflows.

## 🛠️ Architecture

The project is built using a modular architecture to separate "Brain" (LLM) from "Hands" (Tools).

```mermaid
graph TD
    User[User Input] --> App[app.py (Agent Loop)]
    App --> Brain[LLM (OpenAI GPT-4o)]
    App --> Memory[Conversation History]
    
    subgraph "The Tool Belt"
        App --> Audit[audit_tools.py]
        App --> Report[reporting_tools.py]
    end
    
    Audit --> Polars[Polars Engine]
    Report --> PDF[ReportLab / Email]
    
    Polars --> CSV[(sales_data.csv)]
    Audit --> Log[temp_audit_log.txt]
    Log --> Report


💻 Tech Stack
LangChain: For Agent orchestration and ReAct logic.

Polars: For high-performance data processing (blazing fast compared to Pandas).

OpenAI GPT-4o: The reasoning engine.

ReportLab: For programmatic PDF generation.

Based on the image you shared, two things are happening:

You are in "Edit Mode": VS Code shows the raw code by default. To see the pretty version (with diagrams and bold text), you need to click the Preview Button in the top right corner (icon looks like a magnifying glass with lines) or press Ctrl + Shift + V (Windows) / Cmd + Shift + V (Mac).

Copy-Paste Errors: It looks like you accidentally copied the words "Code snippet" and "Bash" from my chat into the file. That breaks the formatting.

Here is the clean, raw Markdown.

Action:

Delete everything currently inside your README.md.

Copy the code block below and paste it in.

Save the file.

Markdown

# 🕵️‍♂️ The Skeptic Analyst Agent

> *"I don't trust your data until I verify it."*

The **Skeptic Analyst** is an autonomous AI agent designed to audit data quality with extreme prejudice. Unlike standard assistants that blindly accept data inputs, this agent assumes all data is "dirty," performs rigorous engineering checks, and refuses to sign off until the data is proven clean.

## 🚀 Key Features

* **Autonomic Vision:** Automatically detects and reads CSV files from the local directory.
* **Paranoid Audit Suite:** Runs 30+ engineering checks including:
    * Null Explosion & Schema Drift
    * Duplicate Detection
    * Outlier Analysis & Range Violations
    * Business Rule Logic (e.g., Invalid Regions)
* **ReAct Cognitive Architecture:** Uses a "Reason + Act" loop to form hypotheses about data errors before verifying them.
* **Professional Reporting:** Generates downloadable **PDF Audit Reports** and simulates email delivery to stakeholders.
* **Memory:** Remembers context across the chat session to handle multi-step workflows.

## 🛠️ Architecture

The project is built using a modular architecture to separate "Brain" (LLM) from "Hands" (Tools).

```mermaid
graph TD
    User[User Input] --> App[app.py (Agent Loop)]
    App --> Brain[LLM (OpenAI GPT-4o)]
    App --> Memory[Conversation History]
    
    subgraph "The Tool Belt"
        App --> Audit[audit_tools.py]
        App --> Report[reporting_tools.py]
    end
    
    Audit --> Polars[Polars Engine]
    Report --> PDF[ReportLab / Email]
    
    Polars --> CSV[(sales_data.csv)]
    Audit --> Log[temp_audit_log.txt]
    Log --> Report
💻 Tech Stack
LangChain: For Agent orchestration and ReAct logic.

Polars: For high-performance data processing (blazing fast compared to Pandas).

OpenAI GPT-4o: The reasoning engine.

ReportLab: For programmatic PDF generation.

⚙️ How to Run

1. Clone the repository:


git clone [https://github.com/nitishbelagali/skeptic-analyst-agent.git](https://github.com/nitishbelagali/skeptic-analyst-agent.git)
cd skeptic-analyst-agent

2. Install dependencies:

pip install -r requirements.txt

3. Set up Environment: Create a .env file and add your OpenAI Key:


OPENAI_API_KEY=sk-your-key-here

4. Run the Agent:

python app.py

🧪 Usage Example
User: "Here is the monthly sales file. Is it good to go?"

Skeptic Agent:

Thought: The user is confident. I should doubt this. I will run the deep audit tool.

Action: run_deep_audit

Result: ❌ FAILED. Found 50 negative sales values and 2 duplicate rows.

Response: "The data is not clean. I found critical errors. Would you like me to generate a PDF report for your manager?"

🔮 Roadmap
[x] Core Audit Logic

[x] PDF & Email Reporting

[ ] Transformation Layer: Auto-cleaning capabilities (Remove duplicates, impute nulls).

[ ] Visualizations: Auto-generate distribution charts.