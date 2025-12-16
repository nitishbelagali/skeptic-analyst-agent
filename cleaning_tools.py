import polars as pl
from typing import List, Dict, Any
from langchain_core.tools import tool

class CleaningSession:
    def __init__(self):
        self.current_df = None
        self.cleaning_history = []
        self.source_filename = None
        self.history_stack = []

    def load_frame(self, df: pl.DataFrame, source_filename: str = None):
        self.current_df = df
        self.source_filename = source_filename
        self.cleaning_history = []
        self.history_stack = []

    def _save_state(self):
        if self.current_df is not None:
            self.history_stack.append(self.current_df.clone())
    
    def analyze_options(self):
        if self.current_df is None: return "❌ No data loaded.", []
        
        df = self.current_df
        issues = []
        
        # 1. Nulls
        for col in df.columns:
            cnt = df[col].null_count()
            if cnt > 0: issues.append({"id": len(issues)+1, "type": "null", "col": col, "count": cnt})

        # 2. Duplicates
        dup = df.is_duplicated().sum()
        if dup > 0: issues.append({"id": len(issues)+1, "type": "duplicate", "count": dup})
        
        # 3. Negatives & Outliers
        num_cols = [c for c in df.columns if df[c].dtype.is_numeric()]
        for col in num_cols:
            if df.filter(pl.col(col) < 0).height > 0:
                issues.append({"id": len(issues)+1, "type": "negative", "col": col})
            
            if df.height > 10:
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                if q1 is not None and q3 is not None and (q3-q1) > 0:
                    upper = q3 + 1.5*(q3-q1)
                    if df.filter(pl.col(col) > upper).height > 0:
                        issues.append({"id": len(issues)+1, "type": "outlier", "col": col})

        if not issues: return "✅ Data is clean!", []

        menu = "🔧 **CLEANING MENU:**\n0. **Auto-Pilot** (Fix All)\n"
        for i in issues:
            menu += f"{i['id']}. Fix {i['type']} in '{i.get('col', 'rows')}'\n"
        
        return menu, issues

    def preview_fix(self, input_str: str):
        """Generates a safety preview message."""
        if self.current_df is None: return "❌ No data."
        
        if "0" in input_str.split():
            n_dups = self.current_df.is_duplicated().sum()
            return f"⚠️ **SAFETY CHECK (Auto-Pilot):**\n- Will remove {n_dups} duplicate rows\n- Will fill missing values (forward fill)\n- Will convert negatives to absolute\n- Will cap outliers at 1.5*IQR\n\nProceed? (yes/no)"
            
        return f"⚠️ **SAFETY CHECK:**\nYou selected: '{input_str}'.\nThis will modify/remove rows based on the selected options.\n\nProceed? (yes/no)"

    def apply_fix(self, raw_input: str):
        """
        Smart Parsing Logic:
        - "0" -> Auto-Pilot (Fixes EVERYTHING)
        - "1, 2, 3" -> IDs: [1, 2, 3]
        - "1 remove" -> ID: 1, Strategy: "remove"
        """
        if self.current_df is None: return "❌ No data."
        self._save_state()
        
        clean_input = raw_input.replace(",", " ").strip()
        parts = clean_input.split()
        if not parts: return "❌ No options selected."
        
        # --- AUTO-PILOT LOGIC (Now Complete) ---
        if "0" in parts:
            # 1. Deduplicate
            self.current_df = self.current_df.unique()
            
            # 2. Fill Nulls (Forward Fill)
            self.current_df = self.current_df.fill_null(strategy="forward")
            
            # 3. Numeric Fixes (Negatives & Outliers)
            num_cols = [c for c in self.current_df.columns if self.current_df[c].dtype.is_numeric()]
            for col in num_cols:
                # Fix Negatives
                self.current_df = self.current_df.with_columns(pl.col(col).abs())
                
                # Cap Outliers
                if self.current_df.height > 10:
                    q1 = self.current_df[col].quantile(0.25)
                    q3 = self.current_df[col].quantile(0.75)
                    if q1 is not None and q3 is not None:
                        iqr = q3 - q1
                        upper = q3 + (1.5 * iqr)
                        lower = q1 - (1.5 * iqr)
                        self.current_df = self.current_df.with_columns(
                            pl.when(pl.col(col) > upper).then(upper)
                            .when(pl.col(col) < lower).then(lower)
                            .otherwise(pl.col(col)).alias(col)
                        )

            return "✅ Auto-Pilot Complete.\n- Duplicates removed\n- Nulls filled\n- Negatives fixed\n- Outliers capped\n\nType 'done' to finish."

        # --- MANUAL LOGIC ---
        ids = []
        strategy = ""
        
        if all(p.isdigit() for p in parts):
            ids = [int(p) for p in parts]
        else:
            if parts[0].isdigit():
                ids = [int(parts[0])]
                strategy = " ".join(parts[1:])
        
        _, current_issues = self.analyze_options()
        applied_log = []
        
        for target_id in ids:
            issue = next((i for i in current_issues if i['id'] == target_id), None)
            if not issue: continue
            
            if issue['type'] == "duplicate":
                self.current_df = self.current_df.unique()
                applied_log.append("Removed duplicates")
                
            elif issue['type'] == "null":
                if "drop" in strategy:
                    self.current_df = self.current_df.drop_nulls(subset=[issue['col']])
                    applied_log.append(f"Dropped nulls in {issue['col']}")
                else:
                    self.current_df = self.current_df.with_columns(pl.col(issue['col']).fill_null(strategy="forward"))
                    applied_log.append(f"Filled nulls in {issue['col']}")
                    
            elif issue['type'] == "negative":
                self.current_df = self.current_df.with_columns(pl.col(issue['col']).abs())
                applied_log.append(f"Fixed negatives in {issue['col']}")
                
            elif issue['type'] == "outlier":
                q3 = self.current_df[issue['col']].quantile(0.75)
                self.current_df = self.current_df.with_columns(
                    pl.when(pl.col(issue['col']) > q3*1.5).then(q3*1.5)
                    .otherwise(pl.col(issue['col'])).alias(issue['col'])
                )
                applied_log.append(f"Capped outliers in {issue['col']}")

        if not applied_log:
            return "❌ Invalid options or no issues found with those IDs."

        new_menu, _ = self.analyze_options()
        return f"✅ **Applied Fixes:**\n" + "\n".join([f"- {x}" for x in applied_log]) + "\n\n" + new_menu

    def export_cleaned_data(self):
        if self.current_df is None: return "No data."
        self.current_df.write_csv("clean_data.csv")
        return "✅ Saved to clean_data.csv"
        
    def get_summary(self):
        return f"{self.current_df.height} rows"

session = CleaningSession()

# --- TOOL WRAPPERS ---
@tool
def clean_data_tool(action_input: str) -> str:
    """Analyzes the current dataset and returns a menu of cleaning options."""
    return session.analyze_options()[0]

@tool
def check_cleaning_options(input_str: str = ""):
    """Checks for data quality issues and returns a formatted menu of recommended fixes."""
    report, _ = session.analyze_options()
    return report

@tool
def preview_cleaning_fix(input_str: str):
    """Provides a dry-run preview of the cleaning action to ensure safety before execution."""
    return session.preview_fix(input_str)

@tool
def apply_cleaning_fix(input_str: str):
    """Applies the selected cleaning option (e.g., '0' for auto-pilot, '1, 2' for manual) to the dataset."""
    return session.apply_fix(input_str)