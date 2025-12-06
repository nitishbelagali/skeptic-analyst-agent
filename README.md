# 🕵️ Skeptic Analyst Agent

**"Trust No Data. Audit Everything."**

The **Skeptic Analyst Agent** is an AI-powered data auditor that refuses to trust CSV files. It automatically scans for engineering errors (schema drift, nulls, duplicates) and business logic violations (negative sales, invalid regions).

Unlike standard tools, it is **paranoid by default** and offers an **Interactive "Data Surgeon" Menu** to fix issues on the fly.

---

## 🚀 Features

### 1. 🔍 Universal Audit
* **Dataset Agnostic:** Works on *any* CSV (Sales, Patients, Titanic, etc.).
* **Deep Inspection:** Detects Schema Drift, Nulls, Duplicates, Negative Values, and Statistical Outliers (IQR).
* **Paranoid Reporting:** Generates a PDF report summarizing every flaw found.

### 2. 🔧 Interactive Data Cleaning (The "Surgeon")
If errors are found, the Agent offers a dynamic menu to fix them:
* **Smart Strategies:** Fill Nulls (Mean/Median/Mode), Cap Outliers, Remove Duplicates.
* **Auto-Pilot:** One-click fix for all standard errors.
* **Safety Net:** Includes an **UNDO** button to revert mistakes instantly.
* **Whitelist Logic:** Learns that "Unknown" is a valid state to prevent audit loops.

### 3. 📂 Smart File Handling
* **Auto-Scan:** Automatically detects all `.csv` files in the directory.
* **Selection Menu:** Asks the user which file to audit.
* **Smart Saving:** Saves cleaned files with a prefix (e.g., `patients.csv` $\rightarrow$ `clean_patients.csv`).

---

## 🛠️ Tech Stack
* **Core:** Python 3.11+
* **Data Engine:** Polars (High-performance DataFrame library)
* **AI Brain:** LangChain + OpenAI (GPT-4o)
* **Reporting:** ReportLab (PDF Generation)

---

## ⚡ Quick Start

### 1. Setup
```bash
# Clone the repo
git clone [https://github.com/nitishbelagali/skeptic-analyst-agent.git](https://github.com/nitishbelagali/skeptic-analyst-agent.git)
cd skeptic-analyst-agent

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

2. Configure Credentials
Create a .env file and add your OpenAI Key:

Code snippet

OPENAI_API_KEY=sk-your-key-here
3. Run the Agent
Bash

python app.py
🎮 How to Use
Select File: The Agent lists available CSVs. Type the number to load one.

Audit: The Agent automatically audits the data.

Choose Action:

1 Download PDF Report

2 Email Report

3 Interactive Cleaning

Clean: Select a fix (e.g., "1 median" to fill nulls with median).

Undo/Export: Type undo if you mess up, or done to switch files.

📂 Project Structure
app.py - Main application with Session Loop & Smart File Selector.

audit_tools.py - Universal logic for detecting data quality issues.

cleaning_tools.py - The "Surgeon" logic (Fixes, Undo, Auto-Pilot).

reporting_tools.py - PDF generation engine.

instructions.txt - System Prompt defining the "Skeptic" persona.

📜 License
MIT License. Use responsibly.


---