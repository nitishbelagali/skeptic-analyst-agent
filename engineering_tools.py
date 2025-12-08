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
        
        Heuristics:
        - Low Cardinality String (< 90% unique) -> Dimension
        - Date/Time -> Time Dimension
        - High Cardinality Numeric -> Fact/Measure
        - Very Low Cardinality Numeric (< 5%, < 20 unique) -> Dimension (e.g., ratings)
        """
        # Validation
        if df is None or df.height == 0:
            return "❌ Cannot detect schema on empty data."
        
        total_rows = df.height
        dimensions = []
        measures = []
        time_dims = []
        
        for col in df.columns:
            # Skip ID columns (they're usually just row indices)
            if col.lower() in ["id", "index", "row_num"]:
                continue
            
            dtype = df[col].dtype
            n_unique = df[col].n_unique()
            cardinality_ratio = n_unique / total_rows if total_rows > 0 else 0
            
            # Rule 1: Date/Time columns -> Time Dimension
            if dtype in [pl.Date, pl.Datetime, pl.Time]:
                time_dims.append(col)
            
            # Rule 2: Text with low cardinality -> Dimension (e.g., City, Region, Blood Type)
            elif dtype == pl.Utf8:  # FIX: Changed from pl.String
                if cardinality_ratio < 0.9:  # Almost unique strings might be names/IDs
                    dimensions.append({
                        "name": col,
                        "cardinality": n_unique,
                        "sample_values": df[col].unique()[:3].to_list()
                    })
            
            # Rule 3: Numbers with very low cardinality -> Dimension (e.g., ratings: 1,2,3,4,5)
            elif dtype.is_numeric() and cardinality_ratio < 0.05 and n_unique < 20:
                dimensions.append({
                    "name": col,
                    "cardinality": n_unique,
                    "sample_values": df[col].unique().sort()[:5].to_list()
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
            "time_dimensions": time_dims,
            "dimension_details": dimensions,
            "measure_details": measures
        }
        
        return self._format_plan_report(dimensions, measures, time_dims)

    def _format_plan_report(self, dims, measures, time_dims):
        """Generates a human-readable schema proposal"""
        dim_list = "\n".join([
            f"  • {d['name']} ({d['cardinality']} unique values, e.g., {', '.join(map(str, d['sample_values']))})"
            for d in dims
        ])
        
        measure_list = "\n".join([
            f"  • {m['name']} (range: {m['stats']['min']:.2f} - {m['stats']['max']:.2f})"
            for m in measures
        ])
        
        time_list = ", ".join(time_dims) if time_dims else "None"
        
        return f"""
🏗️ **PROPOSED DATA WAREHOUSE SCHEMA**

🔹 **DIMENSION TABLES** (Context/Categories):
{dim_list if dims else '  None detected'}

📅 **TIME DIMENSIONS**:
{time_list}

🔸 **FACT TABLE MEASURES** (Numeric Metrics):
{measure_list if measures else '  None detected'}

💡 **Transformation Plan:**
I will create {len(dims)} dimension tables and 1 central fact table.
Each dimension will get a surrogate key (ID), and the fact table will reference these IDs.

Example:
  dim_city (city_id, city_name)
  dim_blood_type (blood_type_id, blood_type)
  fact_patients (fact_id, city_id, blood_type_id, age, weight)
        """.strip()

    def apply_transformation(self, df: pl.DataFrame):
        """
        Splits the dataframe into Star Schema (Fact + Dimension tables).
        """
        if not self.current_schema_plan:
            return "❌ No schema plan found. Run detect_schema() first."
        
        dims = self.current_schema_plan['dimensions']
        
        if not dims:
            return "⚠️ No dimensions detected. Your data might already be in a fact-only format."
        
        self.dim_tables = {}
        
        # Start with the original dataframe as the base for the Fact Table
        self.fact_table = df.clone()
        
        # Process each dimension
        for dim_col in dims:
            try:
                # 1. Create Dimension Table (Unique values + Surrogate Key)
                dim_df = (
                    df.select(dim_col)
                    .unique()
                    .drop_nulls()  # Remove nulls from dimension
                    .sort(dim_col)
                    .with_row_count(name=f"{dim_col}_id", offset=1)
                )
                
                # Store dimension table
                self.dim_tables[f"dim_{dim_col}"] = dim_df
                
                # 2. Join back to Fact Table to get Foreign Keys
                self.fact_table = self.fact_table.join(
                    dim_df, 
                    on=dim_col, 
                    how="left"
                )
                
                # 3. Drop the original text column (keep only the ID)
                self.fact_table = self.fact_table.drop(dim_col)
                
            except Exception as e:
                return f"❌ Error processing dimension '{dim_col}': {e}"
        
        # FIX: Add primary key to fact table
        self.fact_table = self.fact_table.with_row_count(name="fact_id", offset=1)
        
        return f"✅ Transformation Complete!\n  • Created {len(self.dim_tables)} dimension tables\n  • Created 1 fact table with {self.fact_table.height} rows"

    def load_to_duckdb(self):
        """
        Loads the transformed tables into a local DuckDB file.
        """
        if self.fact_table is None:
            return "❌ No transformed data to load. Run apply_transformation() first."
        
        try:
            # Remove old database file if it exists
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
            
            # Connect to DuckDB
            conn = duckdb.connect(self.db_path)
            
            # FIX: Load Dimension Tables (register first!)
            for name, df in self.dim_tables.items():
                conn.register(f"{name}_temp", df)
                conn.execute(f"CREATE TABLE {name} AS SELECT * FROM {name}_temp")
            
            # FIX: Load Fact Table (register first!)
            conn.register("fact_table_temp", self.fact_table)
            conn.execute("CREATE TABLE fact_table AS SELECT * FROM fact_table_temp")
            
            # Get summary of created tables
            tables = conn.execute("SHOW TABLES").fetchall()
            table_list = [t[0] for t in tables]
            
            # Get row counts
            summary = []
            for table in table_list:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                summary.append(f"  • {table}: {count} rows")
            
            conn.close()
            
            return f"""
✅ SUCCESS: Data warehouse created!

📁 Database: {self.db_path}
📊 Tables loaded:
{chr(10).join(summary)}

You can now query this database using SQL or ask me questions about the data.
            """.strip()
            
        except Exception as e:
            return f"❌ Database Error: {e}"

    def query_database(self, sql: str):
        """
        Executes a SQL query on the warehouse.
        Useful for testing or answering user questions.
        """
        if not os.path.exists(self.db_path):
            return "❌ Database doesn't exist. Run load_to_duckdb() first."
        
        try:
            conn = duckdb.connect(self.db_path)
            result = conn.execute(sql).fetchdf()
            conn.close()
            
            if result.empty:
                return "Query returned no results."
            
            return result
            
        except Exception as e:
            return f"❌ Query Error: {e}"
    
    def get_schema_info(self):
        """
        Returns information about the database schema.
        Useful for LLM to understand structure before generating SQL.
        """
        if not os.path.exists(self.db_path):
            return "❌ Database doesn't exist."
        
        try:
            conn = duckdb.connect(self.db_path)
            
            schema_info = "📊 DATABASE SCHEMA:\n\n"
            tables = conn.execute("SHOW TABLES").fetchall()
            
            for table in tables:
                table_name = table[0]
                schema_info += f"🔹 {table_name}\n"
                
                # Get columns
                columns = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                for col in columns:
                    schema_info += f"   • {col[1]} ({col[2]})\n"
                
                schema_info += "\n"
            
            conn.close()
            return schema_info
            
        except Exception as e:
            return f"❌ Error: {e}"
    
    def reset(self):
        """Clears the current session state"""
        self.current_schema_plan = None
        self.fact_table = None
        self.dim_tables = {}
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        return "✅ Session reset. Ready for new data."

# Global Instance
session = EngineeringSession()