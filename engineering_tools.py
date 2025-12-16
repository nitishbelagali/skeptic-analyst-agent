import polars as pl
import duckdb
import os
import re

class EngineeringSession:
    def __init__(self):
        self.db_path = "warehouse.db"
        self.current_schema_plan = None
        self.fact_table = None
        self.dim_tables = {}

    def _clean_name(self, name):
        """Replaces spaces/special chars with underscores for SQL safety."""
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        clean = re.sub(r'_+', '_', clean)
        return clean.strip('_')

    def detect_schema(self, df: pl.DataFrame):
        """Analyzes DataFrame to propose Star Schema."""
        if df is None or df.height == 0:
            return "❌ Cannot detect schema on empty data."
        
        total_rows = df.height
        dimensions = []
        measures = []
        time_dims = []
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Skip ID columns
            if col_lower in ["id", "index", "row_num"] or col_lower.endswith("_id"):
                continue
            
            dtype = df[col].dtype
            n_unique = df[col].n_unique()
            cardinality_ratio = n_unique / total_rows if total_rows > 0 else 0
            
            # Rule 1: Date/Time → Time Dimension
            if dtype in [pl.Date, pl.Datetime, pl.Time] or "date" in col_lower or "time" in col_lower:
                time_dims.append(col)
            
            # Rule 2: Low cardinality String → Dimension
            elif dtype == pl.Utf8 and cardinality_ratio < 0.9:
                dimensions.append({"name": col, "cardinality": n_unique})
            
            # Rule 3: Low cardinality Numeric → Dimension (e.g., ratings 1-5)
            elif dtype.is_numeric() and cardinality_ratio < 0.05 and n_unique < 20:
                dimensions.append({"name": col, "cardinality": n_unique})
            
            # Rule 4: High cardinality Numeric → Measure
            elif dtype.is_numeric():
                measures.append({"name": col})
        
        self.current_schema_plan = {
            "dimensions": [d["name"] for d in dimensions],
            "measures": [m["name"] for m in measures],
            "time_dimensions": time_dims
        }
        
        return self._format_plan_report()

    def modify_schema_plan(self, column_name, new_role, df_context=None):
        """
        Manually moves a column to a new role (dimension/measure).
        INCLUDES SMART VALIDATION to prevent bad schema design.
        """
        if not self.current_schema_plan:
            return "❌ No schema plan found. Run detect_schema first."
        
        col = column_name.strip()
        role = new_role.lower().strip()
        
        # --- SMART VALIDATION ---
        # We need access to data stats to validate. If df_context is passed (it should be), check logic.
        if df_context is not None and col in df_context.columns:
            n_unique = df_context[col].n_unique()
            total = df_context.height
            ratio = n_unique / total if total > 0 else 0
            is_numeric = df_context[col].dtype.is_numeric()

            # Logic 1: Can't make high-cardinality text a FACT
            if ("fact" in role or "measure" in role):
                if not is_numeric:
                    return f"❌ **Request Denied:** '{col}' is text. Facts must be numeric measurements."
                if ratio < 0.05:
                    return f"⚠️ **Warning:** '{col}' has very few unique values ({n_unique}). It is much better suited as a DIMENSION."

            # Logic 2: Can't make unique IDs/Numbers a DIMENSION
            if ("dim" in role):
                if is_numeric and ratio > 0.9:
                    return f"❌ **Request Denied:** '{col}' is unique for almost every row. It cannot be used as a grouping Category (Dimension)."
        # ------------------------

        # Remove from all lists
        self.current_schema_plan['dimensions'] = [x for x in self.current_schema_plan['dimensions'] if x != col]
        self.current_schema_plan['measures'] = [x for x in self.current_schema_plan['measures'] if x != col]
        self.current_schema_plan['time_dimensions'] = [x for x in self.current_schema_plan['time_dimensions'] if x != col]
        
        # Add to new role
        if "dim" in role:
            self.current_schema_plan['dimensions'].append(col)
        elif "measure" in role or "fact" in role:
            self.current_schema_plan['measures'].append(col)
        elif "time" in role:
            self.current_schema_plan['time_dimensions'].append(col)
        else:
            return f"❌ Unknown role: {role}. Use 'dimension', 'measure', or 'time'."
        
        return f"✅ Moved '{col}' to {role}.\n\nNew Plan:\n{self._format_plan_report()}"

    def _format_plan_report(self):
        """Formats schema plan as readable text."""
        dims = self.current_schema_plan.get('dimensions', [])
        measures = self.current_schema_plan.get('measures', [])
        time_dims = self.current_schema_plan.get('time_dimensions', [])
        
        return f"""
**PROPOSED SCHEMA**
DIMENSIONS (Categories): {', '.join(dims) if dims else 'None'}
TIME DIMENSIONS: {', '.join(time_dims) if time_dims else 'None'}
FACT MEASURES (Numbers): {', '.join(measures) if measures else 'None'}
        """.strip()

    def get_schema_diagram(self):
        """Generates Graphviz DOT string for ERD diagram."""
        if not self.current_schema_plan:
            return None
        
        dims = self.current_schema_plan['dimensions'] + self.current_schema_plan['time_dimensions']
        
        # Build DOT syntax
        dot = 'digraph StarSchema {\n'
        dot += '  rankdir=LR;\n'
        dot += '  node [shape=box, style=filled, fontname="Helvetica"];\n'
        dot += '  FACT [label="FACT TABLE", fillcolor="#FFD700", fontsize=12];\n'
        
        for d in dims:
            clean = self._clean_name(d)
            dot += f'  DIM_{clean.upper()} [label="DIM_{clean.upper()}", fillcolor="#ADD8E6", fontsize=10];\n'
            dot += f'  FACT -> DIM_{clean.upper()};\n'
        
        dot += '}'
        return dot

    def apply_transformation(self, df: pl.DataFrame):
        """Splits dataframe into Star Schema (fact + dimension tables)."""
        if not self.current_schema_plan:
            return "❌ No schema plan found. Run detect_schema first."
        
        dims = self.current_schema_plan['dimensions'] + self.current_schema_plan['time_dimensions']
        
        self.dim_tables = {}
        self.fact_table = df.clone()
        self.fact_table = self.fact_table.with_row_count(name="fact_id", offset=1)
        
        for dim_col in dims:
            try:
                clean_col = self._clean_name(dim_col)
                dim_table_name = f"dim_{clean_col}"
                fk_col_name = f"{clean_col}_id"
                
                # Create dimension table
                dim_df = (
                    df.select(dim_col)
                    .unique()
                    .drop_nulls()
                    .sort(dim_col)
                    .with_row_count(name=fk_col_name, offset=1)
                )
                
                self.dim_tables[dim_table_name] = dim_df
                
                # Join back to fact table
                self.fact_table = self.fact_table.join(dim_df, on=dim_col, how="left")
                self.fact_table = self.fact_table.drop(dim_col)
                
            except Exception as e:
                print(f"Warning: Could not process dimension '{dim_col}': {e}")
                continue
        
        return f"✅ Transformation Complete! Created {len(self.dim_tables)} dimension tables + 1 fact table."

    def load_to_duckdb(self):
        """Loads transformed tables into DuckDB with proper cleanup."""
        if self.fact_table is None:
            return "❌ No data to load. Run apply_transformation first."
        
        try:
            if os.path.exists(self.db_path):
                try: os.remove(self.db_path)
                except: pass
            
            conn = duckdb.connect(self.db_path)
            
            for name, df in self.dim_tables.items():
                safe_name = self._clean_name(name)
                # Helper to move Polars to DuckDB
                conn.register(f"{safe_name}_temp", df)
                conn.execute(f"CREATE TABLE {safe_name} AS SELECT * FROM {safe_name}_temp")
                conn.unregister(f"{safe_name}_temp")
            
            conn.register("fact_table_temp", self.fact_table)
            conn.execute("CREATE TABLE fact_table AS SELECT * FROM fact_table_temp")
            conn.unregister("fact_table_temp")
            
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            conn.close()
            
            return f"✅ Data warehouse created!\n📁 Database: {self.db_path}\n📊 Tables: {', '.join(table_names)}"
            
        except Exception as e:
            return f"❌ Database Error: {e}"

    def query_database(self, sql: str):
        if not os.path.exists(self.db_path):
            return "❌ Database doesn't exist. Run load_to_warehouse first."
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            result = conn.execute(sql).fetchdf()
            conn.close()
            return result
        except Exception as e:
            return f"❌ Query Error: {e}"

    def get_schema_info(self):
        if not os.path.exists(self.db_path): return "❌ No database found."
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            tables = conn.execute("SHOW TABLES").fetchall()
            info = "DATABASE SCHEMA:\n\n"
            for t in tables:
                table_name = t[0]
                cols = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                col_names = [c[1] for c in cols]
                info += f"Table {table_name}: {', '.join(col_names)}\n"
            conn.close()
            return info
        except Exception as e:
            return f"❌ Error: {e}"

    def reset(self):
        self.current_schema_plan = None
        self.fact_table = None
        self.dim_tables = {}
        if os.path.exists(self.db_path):
            try: os.remove(self.db_path)
            except: pass

session = EngineeringSession()