# 🤖 The Skeptic Analyst (V2.0)

**An AI Data Engineering Agent that doesn't trust your data.**

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](YOUR_STREAMLIT_APP_URL_HERE)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)

## ⚠️ The Problem with AI Agents
Most data agents are "yes-men." They will happily execute SQL on dirty data, hallucinate trends from null values, and delete production tables if you ask nicely.

## 🛡️ The Solution: "Paranoid Architecture"
**The Skeptic Analyst** is built differently. It assumes input data is flawed until proven innocent.

### ✨ Key Features
* **Dual-Interface:** * 🖥️ **Web UI (Streamlit):** Glassmorphism design with interactive Plotly dashboards.
    * 💻 **CLI (Rich):** Terminal-based hacker mode for rapid auditing.
* **Safety First:** Implements "Dry Run" logic. The agent **previews** deletions and asks for confirmation before modifying data.
* **RAG Integration:** Ingests PDF Data Dictionaries to understand *business context* before writing SQL.
* **ELI5 Mode:** Toggles between "Senior Engineer" technical jargon and "Explain Like I'm 5" analogies (Database = Toy Box).

## 🛠️ Tech Stack
* **Brain:** LangChain + OpenAI GPT-4o-mini
* **Memory:** DuckDB (OLAP SQL) + Polars (High-performance Dataframes)
* **Router:** Fuzzy Matching (`thefuzz`) for natural language intent detection
* **UI:** Streamlit & Rich

## 🚀 Quick Start
1. Clone the repo:
   ```bash
   git clone [https://github.com/YOUR_USERNAME/skeptic_analyst.git](https://github.com/YOUR_USERNAME/skeptic_analyst.git)

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    
3. Run the Terminal Interface:
    ```bash
    streamlit run streamlit_app.py
    
4. Run the Terminal Interface:
     ```bash
     python app.py

📸 Screenshots
<img width="1461" height="765" alt="image" src="https://github.com/user-attachments/assets/f4dec610-d335-4bf9-a237-6f0336f58ed4" />

### 📢 Action 3: The "Persona Takeover" Post
Once the Streamlit link is live and the README looks good, post this to LinkedIn.

**Copy/Paste this text:**

**Headline:** ⚠️ **WARNING: Stop trusting your data.**

Most AI agents are "helpful." They will happily hallucinate insights from a dirty CSV just to make you happy.

I got tired of "yes-man" AI. So I built **The Skeptic Analyst**.

It’s a paranoid Data Engineering agent. It doesn’t trust your inputs. It dry-runs deletions because it assumes you made a mistake. It audits nulls before it even thinks about plotting a chart.

It’s built on LangChain, DuckDB, Polars, and pure suspicion.

**New in V2.0:**
🔹 **Dual-Interface:** Works in both the Terminal (CLI) and Web (Streamlit).
🔹 **ELI5 Mode:** Explains complex schemas using "Lego" analogies.
🔹 **Safety Previews:** A "Dry Run" layer that prevents accidental data loss.

If you want an AI that flatters you, use ChatGPT. If you want one that actually cleans your pipeline, try this.

🔗 **Live Demo:** [Insert Your Streamlit Link Here]
💻 **Code:** [Insert Your GitHub Link Here]

#DataEngineering #AI #LangChain #Python #Streamlit #OpenAI #TheSkeptic

---
