import polars as pl
import duckdb
import os

class EngineeringSession:
    def __init__(self):
        self.db_path = "warehouse.db"
        self.current_schema_plan = None
        self.fact_table = None
        self.dim_tables = {}

    def detect_schema(self, df: pl.DataFrame):
        """
        Analyzes the DataFrame to propose a Star Schema.
        """
        if df is None or df.height == 0:
            return "❌ Cannot detect schema on empty data."
        
        total_rows = df.height
        dimensions = []
        measures = []
        time_dims = []
        
        for col in df.columns:
            # FIX 2: Skip ALL columns ending in _id or named id/index
            col_lower = col.lower()
            if col_lower in ["id", "index", "row_num"] or col_lower.endswith("_id"):
                continue
            
            dtype = df[col].dtype
            n_unique = df[col].n_unique()
            cardinality_ratio = n_unique / total_rows if total_rows > 0 else 0
            
            # Rule 1: Date/Time columns -> Time Dimension
            if dtype in [pl.Date, pl.Datetime, pl.Time]:
                time_dims.append(col)
            
            # Rule 2: Text with low cardinality -> Dimension
            elif dtype == pl.Utf8:
                if cardinality_ratio < 0.9: 
                    dimensions.append({
                        "name": col,
                        "cardinality": n_unique,
                        "sample_values": df[col].unique().to_list()[:3]
                    })
            
            # Rule 3: Numbers with very low cardinality -> Dimension (e.g., ratings)
            elif dtype.is_numeric() and cardinality_ratio < 0.05 and n_unique < 20:
                dimensions.append({
                    "name": col,
                    "cardinality": n_unique,
                    "sample_values": df[col].unique().sort().to_list()[:5]
                })
            
            # Rule 4: High cardinality numeric -> Measure/Fact
            elif dtype.is_numeric():
                measures.append({
                    "name": col,
                    "stats": {
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                        "mean": float(df[col].mean())
                    }
                })
        
        # Store the plan
        self.current_schema_plan = {
            "dimensions": [d["name"] for d in dimensions],
            "measures": [m["name"] for m in measures],
            "time_dimensions": time_dims
        }
        
        return self._format_plan_report(dimensions, measures, time_dims)

    def _format_plan_report(self, dims, measures, time_dims):
        """Generates a human-readable schema proposal"""
        dim_list = "\n".join([f"  • {d['name']} ({d['cardinality']} unique values)" for d in dims])
        measure_list = "\n".join([f"  • {m['name']} (range: {m['stats']['min']:.2f} - {m['stats']['max']:.2f})" for m in measures])
        time_list = ", ".join(time_dims) if time_dims else "None"
        
        return f"""
🏗️ **PROPOSED DATA WAREHOUSE SCHEMA**

🔹 **DIMENSION TABLES** (Context):
{dim_list if dims else '  None detected'}

📅 **TIME DIMENSIONS**:
{time_list}

🔸 **FACT TABLE MEASURES** (Numeric Metrics):
{measure_list if measures else '  None detected'}

💡 **Plan:** I will create {len(dims)} dimension tables and 1 central fact table.
""".strip()

    def apply_transformation(self, df: pl.DataFrame):
        """Splits the dataframe into Star Schema."""
        if not self.current_schema_plan:
            return "❌ No schema plan found."
        
        dims = self.current_schema_plan['dimensions']
        self.dim_tables = {}
        self.fact_table = df.clone()
        
        # FIX: Ensure we generate a primary key for the fact table
        self.fact_table = self.fact_table.with_row_count(name="fact_id", offset=1)
        
        for dim_col in dims:
            try:
                # 1. Create Dimension Table
                dim_df = (
                    df.select(dim_col)
                    .unique()
                    .drop_nulls()
                    .sort(dim_col)
                    .with_row_count(name=f"{dim_col}_id", offset=1)
                )
                self.dim_tables[f"dim_{dim_col}"] = dim_df
                
                # 2. Join back to Fact Table
                self.fact_table = self.fact_table.join(dim_df, on=dim_col, how="left")
                
                # 3. Drop original text, keep ID
                self.fact_table = self.fact_table.drop(dim_col)
                
            except Exception as e:
                return f"❌ Error processing dimension '{dim_col}': {e}"
        
        return f"✅ Transformation Complete! Created {len(self.dim_tables)} Dimensions + 1 Fact Table."

    def load_to_duckdb(self):
        """Loads tables into DuckDB and cleans up."""
        if self.fact_table is None: return "❌ No data to load."
        
        try:
            if os.path.exists(self.db_path): os.remove(self.db_path)
            conn = duckdb.connect(self.db_path)
            
            # Load Dimensions
            for name, df in self.dim_tables.items():
                conn.register(f"{name}_temp", df)
                conn.execute(f"CREATE TABLE {name} AS SELECT * FROM {name}_temp")
                # FIX 1: Clean up temp view
                conn.unregister(f"{name}_temp")
            
            # Load Fact
            conn.register("fact_table_temp", self.fact_table)
            conn.execute("CREATE TABLE fact_table AS SELECT * FROM fact_table_temp")
            conn.unregister("fact_table_temp")
            
            # Summary
            tables = conn.execute("SHOW TABLES").fetchall()
            table_list = [t[0] for t in tables]
            conn.close()
            
            return f"✅ SUCCESS: Loaded to '{self.db_path}'.\nTables: {', '.join(table_list)}"
            
        except Exception as e:
            return f"❌ Database Error: {e}"

    def query_database(self, sql: str):
        if not os.path.exists(self.db_path): return "❌ Database not found."
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