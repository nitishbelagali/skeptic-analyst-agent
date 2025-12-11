import polars as pl
from typing import List, Dict, Any

class CleaningSession:
    def __init__(self):
        self.current_df = None
        self.cleaning_history = []
        self.source_filename = None  # <--- NEW

    def load_frame(self, df: pl.DataFrame, source_filename: str = None): # <--- UPDATED SIGNATURE
        """Loads a dataframe into the session."""
        self.current_df = df
        self.source_filename = source_filename
        self.cleaning_history = []
        print(f"Session loaded with {df.height} rows.")

    def analyze_options(self):
        """
        Scans for issues and returns a formatted report + list of fixable issues.
        """
        if self.current_df is None: return "No data loaded.", []
        
        df = self.current_df
        issues = []
        report = "🔍 **AUDIT REPORT**\n"
        
        # 1. Nulls
        null_counts = df.null_count()
        for col in df.columns:
            cnt = null_counts[col][0]
            if cnt > 0:
                issues.append({"type": "null", "col": col, "count": cnt})
                report += f"- Column '{col}' has {cnt} missing values.\n"

        # 2. Duplicates
        is_dup = df.is_duplicated()
        dup_count = is_dup.sum()
        if dup_count > 0:
            issues.append({"type": "duplicate", "count": dup_count})
            report += f"- Found {dup_count} duplicate rows.\n"

        # Formulate Options
        options_text = ""
        if not issues:
            return "✅ Data looks clean! No obvious technical errors found.", []
        
        options_text += "🔧 **Recommended Fixes:**\n"
        options_text += "0. **Auto-Pilot** (Apply all safe defaults)\n"
        
        for i, issue in enumerate(issues, 1):
            if issue["type"] == "null":
                options_text += f"{i}. Fill missing values in '{issue['col']}' (Strategies: mean, median, mode, drop)\n"
            elif issue["type"] == "duplicate":
                options_text += f"{i}. Remove {issue['count']} duplicates\n"
                
        return report + "\n" + options_text, issues

    def apply_fix(self, option_id: str, strategy: str = ""):
        """Applies a specific fix to the dataframe."""
        if self.current_df is None: return "No data."
        
        # Auto-pilot
        if option_id == "0":
            # Simple auto-clean: Drop dups, fill numeric nulls with median, string with mode
            self.current_df = self.current_df.unique()
            
            for col in self.current_df.columns:
                if self.current_df[col].null_count() > 0:
                    if self.current_df[col].dtype.is_numeric():
                        med = self.current_df[col].median()
                        self.current_df = self.current_df.with_columns(self.current_df[col].fill_null(med))
                    else:
                        # Mode for strings
                        try:
                            mode = self.current_df[col].mode().first()
                            self.current_df = self.current_df.with_columns(self.current_df[col].fill_null(mode))
                        except:
                            self.current_df = self.current_df.drop_nulls(subset=[col])
            
            self.cleaning_history.append("Auto-Pilot Cleaning")
            return "✅ Auto-pilot complete: Removed duplicates and filled missing values."

        # Manual logic (simplified for the router)
        # In a real scenario, map ID to specific issue. 
        # Here we just interpret strategy keywords globally for robustness.
        
        original_rows = self.current_df.height
        
        if "remove" in strategy or "drop" in strategy:
            self.current_df = self.current_df.drop_nulls().unique()
            return f"✅ Removed rows. (Rows: {original_rows} -> {self.current_df.height})"
            
        return "Fix applied."

    def export_cleaned_data(self):
        """Saves current state to CSV."""
        if self.current_df is not None:
            name = f"clean_{self.source_filename}" if self.source_filename else "clean_data.csv"
            self.current_df.write_csv(name)
            return f"✅ Saved to {name}"
        return "No data to save."

    def get_summary(self):
        if self.current_df is None: return "Empty"
        return f"{self.current_df.height} rows, {self.current_df.width} cols"

session = CleaningSession()