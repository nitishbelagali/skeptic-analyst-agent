import matplotlib
# CRITICAL FIX: Force non-interactive backend before importing pyplot
matplotlib.use('Agg') 

import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
import pandas as pd
import os
import warnings

# Suppress annoying warnings
warnings.filterwarnings("ignore")

class VisualizationSession:
    def __init__(self, db_path="warehouse.db"):
        self.db_path = db_path
        # Set style
        sns.set_theme(style="whitegrid")

    def generate_dashboard(self):
        """
        Auto-generates an executive dashboard based on warehouse schema.
        Returns the path to the saved image.
        """
        if not os.path.exists(self.db_path):
            return "Error: Database not found. Run engineering pipeline first."

        conn = None
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            
            # 1. INSPECT SCHEMA
            tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
            
            fact_table = "fact_table"
            dim_tables = [t for t in tables if t.startswith("dim_") and not t.endswith("_temp")]
            
            if fact_table not in tables:
                return "Error: Fact table not found. Run transformation first."
            
            # Get fact table structure
            fact_cols_info = conn.execute(f"DESCRIBE {fact_table}").fetchall()
            fact_col_names = [c[0] for c in fact_cols_info]
            
            # Initialize figure (2x2 grid)
            fig, axes = plt.subplots(2, 2, figsize=(16, 10))
            # PURE TEXT TITLE (No Emojis)
            fig.suptitle('Automated Data Analytics Dashboard', fontsize=20, weight='bold', y=0.98)
            
            # --- CHART 1: TIME SERIES (Top Left) ---
            self._plot_time_series(conn, fact_table, dim_tables, fact_col_names, axes[0, 0])
            
            # --- CHART 2: CATEGORICAL BREAKDOWN (Top Right) ---
            self._plot_category_breakdown(conn, fact_table, dim_tables, fact_col_names, axes[0, 1])
            
            # --- CHART 3: NUMERIC DISTRIBUTION (Bottom Left) ---
            self._plot_distribution(conn, fact_table, fact_cols_info, axes[1, 0])
            
            # --- CHART 4: SUMMARY STATS (Bottom Right) ---
            self._plot_summary_stats(conn, fact_table, dim_tables, axes[1, 1])
            
            # Save dashboard
            output_path = "dashboard_report.png"
            plt.tight_layout(rect=[0, 0, 1, 0.96])
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close(fig) # Explicitly close the figure to free memory
            
            return f"Dashboard generated: {output_path}"
            
        except Exception as e:
            print(f"!!! DASHBOARD ERROR: {e}")
            return f"Dashboard generation failed: {e}"
        finally:
            if conn:
                conn.close()

    def _plot_time_series(self, conn, fact_table, dim_tables, fact_cols, ax):
        """Plot time-based trends"""
        date_dim = next((d for d in dim_tables if "date" in d.lower() or "time" in d.lower()), None)
        
        if not date_dim:
            ax.text(0.5, 0.5, "No Time Dimension Found", ha='center', va='center')
            ax.axis('off')
            return
        
        try:
            col_name = date_dim.replace("dim_", "")
            fk_name = f"{col_name}_id"
            
            if fk_name not in fact_cols:
                raise Exception(f"Foreign key {fk_name} not found")
            
            query = f"""
                SELECT d.{col_name} as date_val, COUNT(*) as count 
                FROM {fact_table} f
                JOIN {date_dim} d ON f.{fk_name} = d.{col_name}_id
                GROUP BY d.{col_name}
                ORDER BY d.{col_name}
            """
            df_trend = conn.execute(query).fetchdf()
            
            if not df_trend.empty:
                df_trend['date_val'] = pd.to_datetime(df_trend['date_val'], errors='coerce')
                df_trend = df_trend.dropna(subset=['date_val'])
                sns.lineplot(data=df_trend, x='date_val', y='count', ax=ax, marker="o")
                ax.set_title("Activity Over Time", fontsize=14, weight='bold')
                ax.tick_params(axis='x', rotation=45)
        except Exception:
            ax.text(0.5, 0.5, "Could not plot trend", ha='center')
            ax.axis('off')

    def _plot_category_breakdown(self, conn, fact_table, dim_tables, fact_cols, ax):
        """Plot top categories"""
        date_dim = next((d for d in dim_tables if "date" in d.lower() or "time" in d.lower()), None)
        cat_dim = next((d for d in dim_tables if d != date_dim), None)
        
        if not cat_dim:
            ax.text(0.5, 0.5, "No Category Dimension", ha='center', va='center')
            ax.axis('off')
            return
        
        try:
            col_name = cat_dim.replace("dim_", "")
            fk_name = f"{col_name}_id"
            
            if fk_name not in fact_cols:
                raise Exception(f"Foreign key {fk_name} not found")
            
            query = f"""
                SELECT d.{col_name} as category, COUNT(*) as count 
                FROM {fact_table} f
                JOIN {cat_dim} d ON f.{fk_name} = d.{col_name}_id
                GROUP BY d.{col_name}
                ORDER BY count DESC
                LIMIT 10
            """
            df_cat = conn.execute(query).fetchdf()
            
            if not df_cat.empty:
                sns.barplot(data=df_cat, x='count', y='category', ax=ax, palette="viridis")
                ax.set_title(f"Top {col_name.title()}s", fontsize=14, weight='bold')
        except Exception:
            ax.text(0.5, 0.5, "Could not plot categories", ha='center')
            ax.axis('off')

    def _plot_distribution(self, conn, fact_table, fact_cols_info, ax):
        """Plot distribution"""
        numeric_col = None
        # FIX: Correctly handle the 6-tuple return from DESCRIBE
        for col_info in fact_cols_info:
            col_name = col_info[0]
            col_type = col_info[1]
            
            if col_name not in ['fact_id'] and not col_name.endswith('_id'):
                if any(x in col_type.upper() for x in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL']):
                    numeric_col = col_name
                    break
        
        if not numeric_col:
            ax.text(0.5, 0.5, "No Numeric Measures", ha='center', va='center')
            ax.axis('off')
            return
        
        try:
            df_dist = conn.execute(f"SELECT {numeric_col} FROM {fact_table} WHERE {numeric_col} IS NOT NULL").fetchdf()
            sns.histplot(data=df_dist, x=numeric_col, kde=True, ax=ax, color="skyblue")
            ax.set_title(f"Distribution of {numeric_col}", fontsize=14, weight='bold')
        except Exception:
            ax.text(0.5, 0.5, "Could not plot distribution", ha='center')
            ax.axis('off')

    def _plot_summary_stats(self, conn, fact_table, dim_tables, ax):
        """Display summary text"""
        ax.axis('off')
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()[0]
            dim_count = len(dim_tables)
            
            # Get numeric summary
            fact_cols = conn.execute(f"DESCRIBE {fact_table}").fetchall()
            numeric_col = None
            # FIX: Correctly handle the 6-tuple return from DESCRIBE
            for col_info in fact_cols:
                col_name = col_info[0]
                col_type = col_info[1]
                
                if col_name not in ['fact_id'] and not col_name.endswith('_id'):
                    if any(x in col_type.upper() for x in ['INT', 'FLOAT', 'DOUBLE']):
                        numeric_col = col_name
                        break
            
            numeric_summary = ""
            if numeric_col:
                stats = conn.execute(f"""
                    SELECT 
                        MIN({numeric_col}) as min_val,
                        MAX({numeric_col}) as max_val,
                        AVG({numeric_col}) as avg_val,
                        SUM({numeric_col}) as total_val
                    FROM {fact_table}
                """).fetchone()
                numeric_summary = f"""
{numeric_col.upper()}:
  Min: {stats[0]:,.2f}
  Max: {stats[1]:,.2f}
  Avg: {stats[2]:,.2f}
  Total: {stats[3]:,.2f}
                """
            
            summary_text = f"""
EXECUTIVE SUMMARY
-----------------
Total Transactions: {row_count}
Dimensions: {dim_count}

Generated by: Skeptic Analyst AI
            """
            ax.text(0.1, 0.5, summary_text, fontsize=12, fontfamily='monospace', 
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
        except Exception:
            ax.text(0.5, 0.5, "Could not generate summary", ha='center')

session = VisualizationSession()