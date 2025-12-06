import polars as pl

class CleaningSession:
    def __init__(self):
        self.current_df = None
        self.history_stack = []
        # UPDATE: Added "Unknown" to the list so the Agent accepts it as a fix
        self.VALID_REGIONS = ["North", "South", "East", "West", "Unknown"]

    def load_frame(self, df: pl.DataFrame):
        """
        Loads the dataframe into the session.
        CRITICAL: Immediately strips whitespace so duplicates are detected correctly.
        """
        clean_df = df.with_columns(pl.col(pl.String).str.strip_chars())
        self.current_df = clean_df
        self.history_stack = []

    def _save_state(self):
        """Push current state to history stack before making changes."""
        if self.current_df is not None:
            self.history_stack.append(self.current_df.clone())

    def undo(self):
        """Reverts the last action."""
        if not self.history_stack:
            return "❌ No actions to undo."
        self.current_df = self.history_stack.pop()
        return "✅ Success: Last action undone. Reverted to previous state."

    def analyze_options(self):
        """Scans the dataframe and generates a dynamic menu of available fixes."""
        if self.current_df is None:
            return "❌ No data loaded.", {}
        
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

        # 2. Check for Negative Sales (Range Violation)
        if "sales" in df.columns and df["sales"].dtype.is_numeric():
            neg_count = df.filter(pl.col("sales") < 0).height
            if neg_count > 0:
                options[str(idx)] = {
                    "type": "negative", "column": "sales",
                    "desc": f"Fix {neg_count} Negative Values in 'sales'", "strategies": ["make positive", "replace with 0", "remove rows"]
                }
                idx += 1

        # 3. Check for Outliers (Using IQR)
        if "sales" in df.columns and df["sales"].dtype.is_numeric():
            q1 = df["sales"].quantile(0.25)
            q3 = df["sales"].quantile(0.75)
            iqr = q3 - q1
            upper_bound = max(q3 + (1.5 * iqr), 10000) # Safety floor
            outliers = df.filter(pl.col("sales") > upper_bound)
            if outliers.height > 0:
                options[str(idx)] = {
                    "type": "outlier", "column": "sales",
                    "desc": f"Fix {outliers.height} Outliers (> {upper_bound:.0f})", "strategies": ["cap at threshold", "remove rows", "replace with median"],
                    "threshold": upper_bound
                }
                idx += 1

        # 4. Check for Invalid Regions
        if "region" in df.columns:
            invalid_regions = df.filter(~pl.col("region").is_in(self.VALID_REGIONS))
            if invalid_regions.height > 0:
                 options[str(idx)] = {
                    "type": "region", "column": "region",
                    "desc": f"Fix {invalid_regions.height} Invalid Regions", "strategies": ["replace with mode", "remove rows", "replace with Unknown"]
                }
                 idx += 1

        # 5. Check for Duplicates
        dup_count = df.is_duplicated().sum()
        if dup_count > 0:
            options[str(idx)] = {
                "type": "duplicates", "desc": f"Remove {dup_count} Duplicate Rows", "strategies": ["remove"]
            }
            idx += 1

        if not options:
            return "✅ No obvious cleaning issues found!", {}

        report = "🔧 **Available Cleaning Options:**\n0. Apply ALL Recommended Fixes (Auto-Pilot)\n"
        for key, val in options.items():
            report += f"{key}. {val['desc']} (Strategies: {', '.join(val['strategies'])})\n"
        
        return report, options

    def apply_fix(self, option_id, strategy=None):
        """Applies a specific fix based on user selection."""
        if self.current_df is None: return "❌ No data loaded."
        report, options = self.analyze_options()
        
        # --- AUTO PILOT ---
        if option_id == "0":
            self._save_state()
            changes = []
            if self._has_option_type(options, 'negative'):
                self._apply_negative_fix("sales", "make positive")
                changes.append("✓ Converted negative sales to positive")
            for opt in options.values():
                if opt['type'] == 'nulls':
                    strat = "median" if self.current_df[opt['column']].dtype.is_numeric() else "mode"
                    self._apply_null_fix(opt['column'], strat)
                    changes.append(f"✓ Fixed Nulls in {opt['column']}")
            if self._has_option_type(options, 'region'):
                self._apply_region_fix("replace with mode")
                changes.append("✓ Fixed invalid regions (set to Mode)")
            for opt in options.values():
                if opt['type'] == 'outlier':
                    self._apply_outlier_fix(opt['threshold'], "cap at threshold")
                    changes.append("✓ Capped Outliers")
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
                return self._apply_outlier_fix(target['threshold'], strategy)[1]
            if target['type'] == 'region': return self._apply_region_fix(strategy)[1]
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
        if strategy == "mean": self.current_df = self.current_df.with_columns(pl.col(col).fill_null(self.current_df[col].mean()))
        elif strategy == "median": self.current_df = self.current_df.with_columns(pl.col(col).fill_null(self.current_df[col].median()))
        elif strategy == "mode": 
            m = self.current_df[col].mode()
            if m.len() > 0: self.current_df = self.current_df.with_columns(pl.col(col).fill_null(m[0]))
        elif strategy == "zero": self.current_df = self.current_df.with_columns(pl.col(col).fill_null(0))
        elif strategy == "drop rows": self.current_df = self.current_df.drop_nulls(subset=[col])
        else: return False, "Unknown strategy"
        return True, f"Fixed nulls in {col}"

    def _apply_negative_fix(self, col, strategy):
        if not strategy: strategy = "make positive"
        strategy = strategy.lower()
        if strategy == "make positive": self.current_df = self.current_df.with_columns(pl.col(col).abs())
        elif strategy == "replace with 0": 
            self.current_df = self.current_df.with_columns(pl.when(pl.col(col) < 0).then(0).otherwise(pl.col(col)).alias(col))
        elif strategy == "remove rows": self.current_df = self.current_df.filter(pl.col(col) >= 0)
        else: return False, "Unknown strategy"
        return True, f"Fixed negative values in {col}"

    def _apply_outlier_fix(self, threshold, strategy):
        if not strategy: strategy = "cap at threshold"
        strategy = strategy.lower()
        if strategy == "remove rows": self.current_df = self.current_df.filter(pl.col("sales") <= threshold)
        elif strategy == "cap at threshold":
            self.current_df = self.current_df.with_columns(pl.when(pl.col("sales") > threshold).then(threshold).otherwise(pl.col("sales")).alias("sales"))
        elif strategy == "replace with median":
            self.current_df = self.current_df.with_columns(pl.when(pl.col("sales") > threshold).then(self.current_df["sales"].median()).otherwise(pl.col("sales")).alias("sales"))
        else: return False, "Unknown strategy"
        return True, "Fixed outliers"

    def _apply_region_fix(self, strategy):
        if not strategy: strategy = "replace with mode"
        strategy = strategy.lower()
        if strategy == "remove rows": self.current_df = self.current_df.filter(pl.col("region").is_in(self.VALID_REGIONS))
        elif strategy == "replace with mode":
            m = self.current_df.filter(pl.col("region").is_in(self.VALID_REGIONS))["region"].mode()
            if m.len() > 0:
                self.current_df = self.current_df.with_columns(pl.when(pl.col("region").is_in(self.VALID_REGIONS)).then(pl.col("region")).otherwise(pl.lit(m[0])).alias("region"))
        elif strategy == "replace with unknown":
            self.current_df = self.current_df.with_columns(pl.when(pl.col("region").is_in(self.VALID_REGIONS)).then(pl.col("region")).otherwise(pl.lit("Unknown")).alias("region"))
        else: return False, "Unknown strategy"
        return True, "Fixed invalid regions"

    def export_cleaned_data(self, filename="clean_sales_data.csv"):
        if self.current_df is None: return "❌ No data"
        self.current_df.write_csv(filename)
        return f"✅ Saved to {filename}"
        
    def get_summary(self):
         return f"Rows: {self.current_df.height}"

session = CleaningSession()