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
        """Helper: Replaces spaces/special chars with underscores."""
        # Replace non-alphanumeric (except underscores) with _
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Remove multiple underscores
        clean = re.sub(r'_+', '_', clean)
        return clean.strip('_')

    def detect_schema(self, df: pl.DataFrame):
        """Analyzes the DataFrame to propose a Star Schema."""
        if df is None or df.height == 0:
            return "❌ Cannot detect schema on empty data."
        
        total_rows = df.height
        dimensions = []
        measures = []
        time_dims = []
        
        for col in df.columns:
            # Skip ID columns
            col_lower = col.lower()
            if col_lower in ["id", "index", "row_num"] or col_lower.endswith("_id"):
                continue
            
            dtype = df[col].dtype
            n_unique = df[col].n_unique()
            cardinality_ratio = n_unique / total_rows if total_rows > 0 else 0
            
            # Rule 1: Date/Time -> Time Dimension
            if dtype in [pl.Date, pl.Datetime, pl.Time]:
                time_dims.append(col)
            
            # Rule 2: Low cardinality String -> Dimension
            elif dtype == pl.Utf8:
                if cardinality_ratio < 0.9: 
                    dimensions.append({
                        "name": col,
                        "cardinality": n_unique
                    })
            
            # Rule 3: Low cardinality Numeric -> Dimension
            elif dtype.is_numeric() and cardinality_ratio < 0.05 and n_unique < 20:
                dimensions.append({
                    "name": col,
                    "cardinality": n_unique
                })
            
            # Rule 4: High cardinality Numeric -> Measure
            elif dtype.is_numeric():
                measures.append({
                    "name": col,
                    "stats": {
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                        "mean": float(df[col].mean())
                    }
                })
        
        self.current_schema_plan = {
            "dimensions": [d["name"] for d in dimensions],
            "measures": [m["name"] for m in measures],
            "time_dimensions": time_dims
        }
        
        return self._format_plan_report(dimensions, measures, time_dims)

    def _format_plan_report(self, dims, measures, time_dims):
        dim_list = "\n".join([f"  - {d['name']} ({d['cardinality']} unique)" for d in dims])
        measure_list = "\n".join([f"  - {m['name']}" for m in measures])
        time_list = ", ".join(time_dims) if time_dims else "None"
        
        return f"""
PROPOSED SCHEMA
---------------
DIMENSIONS:
{dim_list if dims else '  None'}

TIME DIMENSIONS:
{time_list}

FACTS:
{measure_list if measures else '  None'}
""".strip()

    def get_schema_diagram(self):
        """Generates a Graphviz DOT string for the schema diagram."""
        if not self.current_schema_plan:
            return None
            
        dims = self.current_schema_plan['dimensions'] + self.current_schema_plan['time_dimensions']
        
        # Graphviz DOT syntax
        dot = 'digraph StarSchema {\n'
        dot += '  rankdir=LR;\n'
        dot += '  node [shape=box, style=filled, fontname="Helvetica"];\n'
        
        # Fact Node
        dot += '  FACT [label="FACT TABLE", fillcolor="#FFD700", fontsize=12];\n'
        
        # Dimension Nodes & Edges
        for d in dims:
            # SANITIZE NAME FOR DIAGRAM
            clean_col = self._clean_name(d)
            table_name = f"DIM_{clean_col.upper()}"
            dot += f'  {table_name} [label="{table_name}", fillcolor="#ADD8E6", fontsize=10];\n'
            dot += f'  FACT -> {table_name} [label="has"];\n'
            
        dot += '}'
        return dot

    def apply_transformation(self, df: pl.DataFrame):
        """Splits the dataframe into Star Schema with SANITIZED names."""
        if not self.current_schema_plan:
            return "❌ No schema plan found."
        
        dims = self.current_schema_plan['dimensions'] + self.current_schema_plan['time_dimensions']
        
        self.dim_tables = {}
        self.fact_table = df.clone()
        
        # Add Surrogate Key (Fact ID)
        self.fact_table = self.fact_table.with_row_count(name="fact_id", offset=1)
        
        for dim_col in dims:
            try:
                # SANITIZE NAMES
                clean_col = self._clean_name(dim_col)
                dim_table_name = f"dim_{clean_col}"
                fk_col_name = f"{clean_col}_id"
                
                # 1. Create Dimension Table
                dim_df = (
                    df.select(dim_col)
                    .unique()
                    .drop_nulls()
                    .sort(dim_col)
                    .with_row_count(name=fk_col_name, offset=1)
                )
                self.dim_tables[dim_table_name] = dim_df
                
                # 2. Join back to Fact Table
                self.fact_table = self.fact_table.join(dim_df, on=dim_col, how="left")
                
                # 3. Drop original column, keep sanitized ID
                self.fact_table = self.fact_table.drop(dim_col)
                
            except Exception as e:
                return f"❌ Error processing '{dim_col}': {e}"
        
        return f"✅ Transformation Complete! Created {len(self.dim_tables)} Dimensions + 1 Fact Table."

    def load_to_duckdb(self):
        if self.fact_table is None: return "❌ No data to load."
        
        try:
            if os.path.exists(self.db_path): os.remove(self.db_path)
            conn = duckdb.connect(self.db_path)
            
            for name, df in self.dim_tables.items():
                # Sanitize table name just in case
                safe_name = self._clean_name(name)
                conn.register(f"{safe_name}_temp", df)
                conn.execute(f"CREATE TABLE {safe_name} AS SELECT * FROM {safe_name}_temp")
                conn.unregister(f"{safe_name}_temp")
            
            conn.register("fact_table_temp", self.fact_table)
            conn.execute("CREATE TABLE fact_table AS SELECT * FROM fact_table_temp")
            conn.unregister("fact_table_temp")
            
            tables = conn.execute("SHOW TABLES").fetchall()
            table_list = [t[0] for t in tables]
            conn.close()
            return f"✅ Loaded to DB. Tables: {', '.join(table_list)}"
            
        except Exception as e: return f"❌ Database Error: {e}"

    def query_database(self, sql: str):
        if not os.path.exists(self.db_path): return "❌ No DB found."
        try:
            conn = duckdb.connect(self.db_path)
            result = conn.execute(sql).fetchdf()
            conn.close()
            return result
        except Exception as e: return f"❌ Query Error: {e}"

    def get_schema_info(self):
        if not os.path.exists(self.db_path): return "❌ No DB found."
        try:
            conn = duckdb.connect(self.db_path)
            tables = conn.execute("SHOW TABLES").fetchall()
            info = ""
            for t in tables:
                cols = conn.execute(f"PRAGMA table_info('{t[0]}')").fetchall()
                info += f"Table {t[0]}: {', '.join([c[1] for c in cols])}\n"
            conn.close()
            return info
        except Exception: return "Error getting schema."

    def reset(self):
        self.current_schema_plan = None
        self.fact_table = None
        self.dim_tables = {}
        if os.path.exists(self.db_path): os.remove(self.db_path)

session = EngineeringSession()