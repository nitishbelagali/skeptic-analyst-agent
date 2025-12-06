import polars as pl

class CleaningSession:
    def __init__(self):
        self.current_df = None
        self.history_stack = []
        # Whitelist "Unknown" to prevent infinite audit loops
        self.VALID_REGIONS = ["North", "South", "East", "West", "Unknown"]

    def load_frame(self, df: pl.DataFrame, source_filename="data.csv"):
        """
        Loads data, fixes empty strings, strips whitespace, and sets output filename.
        """
        # 1. Smart Output Name
        self.output_filename = f"clean_{source_filename}"

        # 2. Strip Whitespace
        clean_df = df.with_columns(pl.col(pl.String).str.strip_chars())
        
        # 3. Convert Empty Strings "" to Nulls (Crucial for CSVs)
        clean_df = clean_df.with_columns([
            pl.when(pl.col(pl.String) == "")
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        ])
        
        self.current_df = clean_df
        self.history_stack = []

    def _save_state(self):
        if self.current_df is not None:
            self.history_stack.append(self.current_df.clone())

    def undo(self):
        if not self.history_stack: return "❌ No actions to undo."
        self.current_df = self.history_stack.pop()
        return "✅ Success: Reverted to previous state."

    def analyze_options(self):
        """Scans ANY column for statistical anomalies."""
        if self.current_df is None: return "❌ No data loaded.", {}
        df = self.current_df
        options = {}
        idx = 1
        
        # 1. Check for Nulls
        for col in df.columns:
            if df[col].null_count() > 0:
                dtype = df[col].dtype
                strategies = ["mean", "median", "mode", "zero"] if dtype.is_numeric() else ["mode", "drop rows"]
                options[str(idx)] = {
                    "type": "nulls", "column": col,
                    "desc": f"Fix {df[col].null_count()} Nulls in '{col}'", "strategies": strategies
                }
                idx += 1

        # 2. Check Numeric Columns
        numeric_cols = [col for col in df.columns if df[col].dtype.is_numeric()]
        
        for col in numeric_cols:
            # A. Negatives
            neg_count = df.filter(pl.col(col) < 0).height
            if neg_count > 0:
                options[str(idx)] = {
                    "type": "negative", "column": col,
                    "desc": f"Fix {neg_count} Negative Values in '{col}'", 
                    "strategies": ["make positive", "replace with 0", "remove rows"]
                }
                idx += 1

            # B. Outliers (Threshold lowered to 4 rows for small test files)
            if df.height >= 4: 
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                if q1 is not None and q3 is not None:
                    iqr = q3 - q1
                    if iqr > 0:
                        upper_bound = q3 + (1.5 * iqr)
                        outliers = df.filter(pl.col(col) > upper_bound)
                        if outliers.height > 0:
                            options[str(idx)] = {
                                "type": "outlier", "column": col,
                                "desc": f"Fix {outliers.height} Outliers in '{col}' (> {upper_bound:.1f})", 
                                "strategies": ["cap at threshold", "remove rows", "replace with median"],
                                "threshold": upper_bound
                            }
                            idx += 1

        # 3. Check for Duplicates
        dup_count = df.is_duplicated().sum()
        if dup_count > 0:
            options[str(idx)] = {
                "type": "duplicates", "desc": f"Remove {dup_count} Duplicate Rows", "strategies": ["remove"]
            }
            idx += 1

        if not options: return "✅ No obvious cleaning issues found!", {}

        report = "🔧 **Available Cleaning Options:**\n0. Apply ALL Recommended Fixes (Auto-Pilot)\n"
        for key, val in options.items():
            report += f"{key}. {val['desc']} (Strategies: {', '.join(val['strategies'])})\n"
        return report, options

    def apply_fix(self, option_id, strategy=None):
        if self.current_df is None: return "❌ No data loaded."
        report, options = self.analyze_options()
        
        # --- AUTO PILOT ---
        if option_id == "0":
            self._save_state()
            changes = []
            
            # Order: Negatives -> Nulls -> Outliers -> Duplicates
            for opt in options.values():
                if opt['type'] == 'negative':
                    success, msg = self._apply_negative_fix(opt['column'], "make positive")
                    if success: changes.append(f"✓ Fixed negatives in '{opt['column']}'")

            for opt in options.values():
                if opt['type'] == 'nulls':
                    strat = "median" if self.current_df[opt['column']].dtype.is_numeric() else "mode"
                    success, msg = self._apply_null_fix(opt['column'], strat)
                    if success: changes.append(f"✓ Fixed Nulls in '{opt['column']}'")

            for opt in options.values():
                if opt['type'] == 'outlier':
                    success, msg = self._apply_outlier_fix(opt['column'], opt['threshold'], "cap at threshold")
                    if success: changes.append(f"✓ Capped Outliers in '{opt['column']}'")
            
            if self._has_option_type(options, 'duplicates'):
                self.current_df = self.current_df.unique()
                changes.append("✓ Removed Duplicates")
                
            return "✅ Auto-pilot complete:\n" + "\n".join(changes) if changes else "⚠️ Auto-pilot ran but no changes were needed."

        # --- MANUAL FIX ---
        if option_id not in options: return "❌ Invalid Option ID."
        target = options[option_id]
        self._save_state()

        try:
            if target['type'] == 'nulls': return self._apply_null_fix(target['column'], strategy)[1]
            if target['type'] == 'negative': return self._apply_negative_fix(target['column'], strategy)[1]
            if target['type'] == 'outlier': 
                if strategy == "median": strategy = "replace with median"
                return self._apply_outlier_fix(target['column'], target['threshold'], strategy)[1]
            if target['type'] == 'duplicates':
                self.current_df = self.current_df.unique()
                return "✅ Duplicates removed."
        except Exception as e:
            self.history_stack.pop()
            return f"❌ Error: {e}"
        return "❌ Action not recognized."

    # --- HELPERS ---
    def _has_option_type(self, options, type_name):
        return any(opt['type'] == type_name for opt in options.values())

    def _apply_null_fix(self, col, strategy):
        if not strategy: strategy = "mode"
        strategy = strategy.lower()
        try:
            if strategy == "mean": self.current_df = self.current_df.with_columns(pl.col(col).fill_null(self.current_df[col].mean()))
            elif strategy == "median": self.current_df = self.current_df.with_columns(pl.col(col).fill_null(self.current_df[col].median()))
            elif strategy == "mode": 
                m = self.current_df[col].mode()
                if m.len() > 0: self.current_df = self.current_df.with_columns(pl.col(col).fill_null(m[0]))
            elif strategy == "zero": self.current_df = self.current_df.with_columns(pl.col(col).fill_null(0))
            elif strategy == "drop rows": self.current_df = self.current_df.drop_nulls(subset=[col])
            else: return False, "Unknown strategy"
            return True, f"Fixed nulls in {col}"
        except Exception: return False, "Error fixing nulls"

    def _apply_negative_fix(self, col, strategy):
        if not strategy: strategy = "make positive"
        strategy = strategy.lower()
        try:
            if strategy == "make positive": self.current_df = self.current_df.with_columns(pl.col(col).abs())
            elif strategy == "replace with 0": 
                self.current_df = self.current_df.with_columns(pl.when(pl.col(col) < 0).then(0).otherwise(pl.col(col)).alias(col))
            elif strategy == "remove rows": self.current_df = self.current_df.filter(pl.col(col) >= 0)
            else: return False, "Unknown strategy"
            return True, f"Fixed negative values in {col}"
        except Exception: return False, "Error fixing negatives"

    def _apply_outlier_fix(self, col, threshold, strategy):
        if not strategy: strategy = "cap at threshold"
        strategy = strategy.lower()
        try:
            if strategy == "remove rows": self.current_df = self.current_df.filter(pl.col(col) <= threshold)
            elif strategy == "cap at threshold":
                self.current_df = self.current_df.with_columns(pl.when(pl.col(col) > threshold).then(threshold).otherwise(pl.col(col)).alias(col))
            elif strategy == "replace with median":
                self.current_df = self.current_df.with_columns(pl.when(pl.col(col) > threshold).then(self.current_df[col].median()).otherwise(pl.col(col)).alias(col))
            else: return False, "Unknown strategy"
            return True, f"Fixed outliers in {col}"
        except Exception: return False, "Error fixing outliers"

    def export_cleaned_data(self, filename=None):
        """Exports to the original smart filename (e.g. clean_patients.csv) unless overridden."""
        if self.current_df is None: return "❌ No data"
        
        target_file = filename if filename else self.output_filename
        self.current_df.write_csv(target_file)
        return f"✅ Saved to {target_file}"
        
    def get_summary(self):
        if self.current_df is None: return "❌ No data loaded."
        df = self.current_df
        null_count = sum(df[col].null_count() for col in df.columns)
        dup_count = df.is_duplicated().sum()
        return f"Rows: {df.height} | Columns: {df.width} | Nulls: {null_count} | Duplicates: {dup_count}"

session = CleaningSession()