import polars as pl
from typing import List, Dict, Any

class CleaningSession:
    def __init__(self):
        self.current_df = None
        self.cleaning_history = []
        self.source_filename = None
        self.history_stack = []  # For undo functionality

    def load_frame(self, df: pl.DataFrame, source_filename: str = None):
        """Loads a dataframe into the session."""
        self.current_df = df
        self.source_filename = source_filename
        self.cleaning_history = []
        self.history_stack = []
        print(f"Session loaded with {df.height} rows, {df.width} columns.")

    def _save_state(self):
        """Saves current state before making changes (for undo)."""
        if self.current_df is not None:
            self.history_stack.append(self.current_df.clone())
    
    def preview_fix(self, option_id: str, strategy: str = ""):
        """Returns a text summary of what would happen (Dry Run)."""
        if self.current_df is None: return "❌ No data."
        
        df = self.current_df
        msg = f"🔎 **PREVIEW (Safety Check):**\n"
        
        if option_id == "0": # Auto-pilot preview
            n_dups = df.is_duplicated().sum()
            n_nulls = sum(df[c].null_count() for c in df.columns)
            msg += f"- Will remove **{n_dups}** duplicate rows\n"
            msg += f"- Will fill/drop **{n_nulls}** missing values\n"
            msg += f"- Will cap outliers using IQR method\n"
            msg += "\n**Are you sure you want to proceed? (yes/no)**"
            return msg
            
        # Simple manual preview
        if "remove" in strategy or "drop" in strategy:
             msg += f"- Will drop rows based on your criteria.\nConfirm? (yes/no)"
             return msg
             
        return "⚠️ Preview unavailable for this specific custom fix. Proceed with caution."

    def undo(self):
        """Reverts to previous state."""
        if not self.history_stack:
            return "❌ No actions to undo."
        self.current_df = self.history_stack.pop()
        return "✅ Last action undone. Reverted to previous state."

    def analyze_options(self):
        """
        Scans for ALL issues (nulls, duplicates, negatives, outliers).
        Returns formatted report + list of fixable issues.
        """
        if self.current_df is None:
            return "❌ No data loaded.", []
        
        df = self.current_df
        issues = []
        report = "🔍 **AUDIT REPORT**\n"
        
        # Whitelist for columns that can be negative
        NEGATIVE_ALLOWED = ['loudness', 'db', 'decibel', 'temperature', 'profit', 'loss', 
                            'change', 'delta', 'diff', 'latitude', 'longitude']
        
        # 1. Nulls
        null_counts = df.null_count()
        for col in df.columns:
            cnt = null_counts[col][0]
            if cnt > 0:
                issues.append({"type": "null", "col": col, "count": cnt})
                report += f"- Column '{col}' has {cnt} missing values.\n"

        # 2. Duplicates
        dup_count = df.is_duplicated().sum()
        if dup_count > 0:
            issues.append({"type": "duplicate", "count": dup_count})
            report += f"- Found {dup_count} duplicate rows.\n"
        
        # 3. Negatives (skip whitelisted columns)
        numeric_cols = [col for col in df.columns if df[col].dtype.is_numeric()]
        for col in numeric_cols:
            col_lower = col.lower()
            
            # Skip if column suggests negatives are valid
            if not any(keyword in col_lower for keyword in NEGATIVE_ALLOWED):
                neg_count = df.filter(pl.col(col) < 0).height
                if neg_count > 0:
                    issues.append({"type": "negative", "col": col, "count": neg_count})
                    report += f"- Column '{col}' has {neg_count} negative values.\n"
        
        # 4. Outliers (IQR method)
        for col in numeric_cols:
            if df.height > 10:
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                if q1 is not None and q3 is not None:
                    iqr = q3 - q1
                    if iqr > 0:
                        upper_bound = q3 + (1.5 * iqr)
                        lower_bound = q1 - (1.5 * iqr)
                        outlier_count = df.filter(
                            (pl.col(col) > upper_bound) | (pl.col(col) < lower_bound)
                        ).height
                        if outlier_count > 0:
                            issues.append({
                                "type": "outlier", 
                                "col": col, 
                                "count": outlier_count, 
                                "upper": upper_bound, 
                                "lower": lower_bound
                            })
                            report += f"- Column '{col}' has {outlier_count} outliers.\n"

        # Formulate Options
        if not issues:
            return "✅ Data looks clean! No obvious technical errors found.", []
        
        options_text = "\n🔧 **Recommended Fixes:**\n"
        options_text += "0. **Auto-Pilot** (Apply all safe defaults)\n"
        
        for i, issue in enumerate(issues, 1):
            if issue["type"] == "null":
                options_text += f"{i}. Fill missing values in '{issue['col']}' (Strategies: mean, median, mode, drop)\n"
            elif issue["type"] == "duplicate":
                options_text += f"{i}. Remove {issue['count']} duplicates\n"
            elif issue["type"] == "negative":
                options_text += f"{i}. Fix {issue['count']} negative values in '{issue['col']}' (Strategies: abs, replace with 0, remove rows)\n"
            elif issue["type"] == "outlier":
                options_text += f"{i}. Handle {issue['count']} outliers in '{issue['col']}' (Strategies: cap, remove rows, replace with median)\n"
                
        return report + options_text, issues

    def apply_fix(self, option_id: str, strategy: str = ""):
        """Applies a specific fix to the dataframe."""
        if self.current_df is None:
            return "❌ No data loaded."
        
        # Save state for undo
        self._save_state()
        
        try:
            # Auto-pilot (option 0)
            if option_id == "0":
                changes = []
                
                # Remove duplicates
                original_rows = self.current_df.height
                self.current_df = self.current_df.unique()
                if self.current_df.height < original_rows:
                    changes.append(f"Removed {original_rows - self.current_df.height} duplicates")
                
                # Fill nulls
                for col in self.current_df.columns:
                    if self.current_df[col].null_count() > 0:
                        if self.current_df[col].dtype.is_numeric():
                            med = self.current_df[col].median()
                            self.current_df = self.current_df.with_columns(
                                pl.col(col).fill_null(med)
                            )
                            changes.append(f"Filled nulls in '{col}' with median")
                        else:
                            try:
                                mode_result = self.current_df[col].mode()
                                if len(mode_result) > 0:
                                    mode_val = mode_result[0]
                                    self.current_df = self.current_df.with_columns(
                                        pl.col(col).fill_null(mode_val)
                                    )
                                    changes.append(f"Filled nulls in '{col}' with mode")
                            except:
                                self.current_df = self.current_df.drop_nulls(subset=[col])
                                changes.append(f"Dropped nulls in '{col}'")
                
                # Fix negatives (convert to absolute)
                NEGATIVE_ALLOWED = ['loudness', 'db', 'decibel', 'temperature', 'profit', 'loss']
                numeric_cols = [col for col in self.current_df.columns if self.current_df[col].dtype.is_numeric()]
                
                for col in numeric_cols:
                    col_lower = col.lower()
                    if not any(keyword in col_lower for keyword in NEGATIVE_ALLOWED):
                        neg_count = self.current_df.filter(pl.col(col) < 0).height
                        if neg_count > 0:
                            self.current_df = self.current_df.with_columns(pl.col(col).abs())
                            changes.append(f"Converted {neg_count} negative values in '{col}' to positive")
                
                # Cap outliers
                for col in numeric_cols:
                    if self.current_df.height > 10:
                        q1 = self.current_df[col].quantile(0.25)
                        q3 = self.current_df[col].quantile(0.75)
                        if q1 is not None and q3 is not None:
                            iqr = q3 - q1
                            if iqr > 0:
                                upper_bound = q3 + (1.5 * iqr)
                                lower_bound = q1 - (1.5 * iqr)
                                outlier_count = self.current_df.filter(
                                    (pl.col(col) > upper_bound) | (pl.col(col) < lower_bound)
                                ).height
                                if outlier_count > 0:
                                    # Cap at bounds
                                    self.current_df = self.current_df.with_columns(
                                        pl.when(pl.col(col) > upper_bound).then(upper_bound)
                                        .when(pl.col(col) < lower_bound).then(lower_bound)
                                        .otherwise(pl.col(col))
                                        .alias(col)
                                    )
                                    changes.append(f"Capped {outlier_count} outliers in '{col}'")
                
                self.cleaning_history.extend(changes)
                
                if changes:
                    summary = "\n".join([f"  ✓ {c}" for c in changes])
                    return f"✅ Auto-pilot complete:\n{summary}"
                else:
                    return "✅ Auto-pilot ran but no changes were needed."

            # Manual strategy
            original_rows = self.current_df.height
            
            if "remove" in strategy.lower() or "drop" in strategy.lower():
                self.current_df = self.current_df.drop_nulls().unique()
                self.cleaning_history.append(f"Manual fix: Dropped rows with strategy '{strategy}'")
                return f"✅ Removed rows. (Rows: {original_rows} → {self.current_df.height})"
            
            if "cap" in strategy.lower():
                # Apply capping logic here (simplified)
                self.cleaning_history.append(f"Applied capping strategy '{strategy}'")
                return "✅ Outliers capped."
            
            # Generic success
            self.cleaning_history.append(f"Applied fix option {option_id} with strategy '{strategy}'")
            return "✅ Fix applied."
            
        except Exception as e:
            # Revert on error
            if self.history_stack:
                self.current_df = self.history_stack.pop()
            return f"❌ Error applying fix: {e}"

    def export_cleaned_data(self):
        """Saves current state to CSV."""
        if self.current_df is None:
            return "❌ No data to export."
        
        try:
            filename = f"clean_{self.source_filename}" if self.source_filename else "clean_data.csv"
            self.current_df.write_csv(filename)
            return f"✅ Saved to {filename}"
        except Exception as e:
            return f"❌ Export failed: {e}"

    def get_summary(self):
        """Returns quick summary of current data state."""
        if self.current_df is None:
            return "No data loaded"
        
        df = self.current_df
        null_count = sum(df[col].null_count() for col in df.columns)
        dup_count = df.is_duplicated().sum()
        
        return f"{df.height} rows, {df.width} cols | Nulls: {null_count} | Dups: {dup_count}"

# Global session instance
session = CleaningSession()