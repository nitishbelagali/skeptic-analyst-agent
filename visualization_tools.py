import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import pandas as pd
import os
from datetime import datetime
from langchain_core.tools import tool
# IMPORT CLEANING SESSION FOR SELF-HEALING
from cleaning_tools import session as cleaning_session

class VisualizationSession:
    def __init__(self, db_path="warehouse.db"):
        self.db_path = db_path
        self.last_figure = None
        self.colors = {
            'primary': '#334155', 'secondary': '#6366f1', 'accent': '#0ea5e9',
            'success': '#10b981', 'background': '#f8fafc', 'grid': '#e2e8f0'
        }

    def _ensure_db_exists(self):
        """SELF-HEALING: If DB is missing, create it instantly from memory."""
        if os.path.exists(self.db_path): return True
        if cleaning_session.current_df is not None:
            try:
                conn = duckdb.connect(self.db_path)
                df_pandas = cleaning_session.current_df.to_pandas()
                conn.register('df_view', df_pandas)
                conn.execute("CREATE TABLE fact_table AS SELECT * FROM df_view")
                conn.close()
                return True
            except: return False
        return False

    def generate_dashboard(self, context=""):
        # 1. Self-Heal
        if not self._ensure_db_exists():
            return "❌ Error: Data not found."

        conn = None
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            
            # --- NEW: Get Summary Stats for the Agent ---
            fact_table = "fact_table"
            row_count = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()[0]
            
            # Try to get top category for insights
            insight_text = f"Analyzed {row_count:,} records."
            try:
                cols = [c[0] for c in conn.execute(f"DESCRIBE {fact_table}").fetchall()]
                cat_col = next((c for c in cols if "id" not in c.lower() and "date" not in c.lower()), None)
                if cat_col:
                    top = conn.execute(f"SELECT {cat_col}, COUNT(*) FROM {fact_table} GROUP BY 1 ORDER BY 2 DESC LIMIT 1").fetchone()
                    insight_text += f" Top {cat_col}: {top[0]} ({top[1]:,} occurrences)."
            except: pass
            
            # Get table info for charts
            tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
            dim_tables = [t for t in tables if t.startswith("dim_")]
            fact_cols_info = conn.execute(f"DESCRIBE {fact_table}").fetchall()

            # Determine mode
            mode = "General Overview"
            if context:
                context_lower = context.lower()
                if any(x in context_lower for x in ["trend", "time"]): mode = "Trend Analysis"
                elif any(x in context_lower for x in ["category", "genre"]): mode = "Category Analysis"

            # Create layout
            fig = make_subplots(
                rows=2, cols=2, column_widths=[0.65, 0.35], row_heights=[0.5, 0.5],
                subplot_titles=(f'{mode}', 'Category Breakdown', 'Distribution', 'Metrics'),
                specs=[[{"type": "xy"}, {"type": "xy"}], [{"type": "xy"}, {"type": "table"}]]
            )
            
            self._add_primary_chart(fig, conn, fact_table, dim_tables, context, 1, 1)
            self._add_category_chart(fig, conn, fact_table, dim_tables, 1, 2)
            self._add_distribution_chart(fig, conn, fact_table, fact_cols_info, 2, 1)
            self._add_summary_table(fig, conn, fact_table, dim_tables, 2, 2)
            
            fig.update_layout(title_text=f"<b>{mode}</b>", height=950, template="plotly_white")
            self.last_figure = fig
            
            output_path = "dashboard_report.html"
            fig.write_html(output_path, config={'displayModeBar': False})
            conn.close()
            
            # Return both Path AND Insights
            return f"PATH:{output_path}|STATS:{insight_text}"
            
        except Exception as e:
            if conn: conn.close()
            return f"❌ Dashboard Error: {e}"

    def generate_dashboard_figure(self):
        if self.last_figure: return self.last_figure
        if "PATH:" in self.generate_dashboard(): return self.last_figure
        return None

    def _add_primary_chart(self, fig, conn, fact_table, dim_tables, context, row, col):
        cols = [c[0] for c in conn.execute(f"DESCRIBE {fact_table}").fetchall()]
        date_dim = next((d for d in dim_tables if "date" in d), None)
        flat_date = next((c for c in cols if "date" in c.lower()), None)
        
        context_lower = context.lower() if context else ""
        
        if ("trend" in context_lower) or flat_date or date_dim:
            if date_dim:
                col_name = date_dim.replace("dim_", "")
                query = f"SELECT d.{col_name} as x, COUNT(*) as y FROM {fact_table} f JOIN {date_dim} d ON f.{col_name}_id = d.{col_name}_id GROUP BY 1 ORDER BY 1"
            elif flat_date:
                query = f"SELECT {flat_date} as x, COUNT(*) as y FROM {fact_table} GROUP BY 1 ORDER BY 1"
            else: query = None
            
            if query:
                try:
                    df = conn.execute(query).fetchdf()
                    if not df.empty:
                        df['x'] = pd.to_datetime(df['x'], errors='coerce')
                        df = df.dropna(subset=['x']).sort_values('x')
                        fig.add_trace(go.Scatter(x=df['x'], y=df['y'], fill='tozeroy', name="Trend"), row=row, col=col)
                except: pass
                return

        self._add_category_chart(fig, conn, fact_table, dim_tables, row, col)

    def _add_category_chart(self, fig, conn, fact_table, dim_tables, row, col):
        cols = [c[0] for c in conn.execute(f"DESCRIBE {fact_table}").fetchall()]
        cat_col = next((c for c in cols if "id" not in c.lower() and "date" not in c.lower()), None)
        
        if cat_col:
            try:
                query = f"SELECT {cat_col} as x, COUNT(*) as y FROM {fact_table} GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
                df = conn.execute(query).fetchdf()
                if not df.empty:
                    fig.add_trace(go.Bar(x=df['y'], y=df['x'], orientation='h', name=cat_col), row=row, col=col)
            except: pass

    def _add_distribution_chart(self, fig, conn, fact_table, info, row, col):
        num_col = next((c[0] for c in info if c[1] in ['BIGINT','DOUBLE'] and 'id' not in c[0].lower()), None)
        if num_col:
            try:
                df = conn.execute(f"SELECT {num_col} FROM {fact_table}").fetchdf()
                fig.add_trace(go.Histogram(x=df[num_col], name=num_col), row=row, col=col)
            except: pass

    def _add_summary_table(self, fig, conn, fact_table, dim_tables, row, col):
        try:
            cnt = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()[0]
            fig.add_trace(go.Table(
                header=dict(values=['Metric','Value']),
                cells=dict(values=[['Rows','Tables'], [f"{cnt:,}", str(len(dim_tables)+1)]])
            ), row=row, col=col)
        except: pass

session = VisualizationSession()

# ------------------------------------------------------------------------------
# LANGCHAIN TOOL WRAPPER
# ------------------------------------------------------------------------------
@tool
def visualize_data_tool(user_request: str) -> str:
    """Generates an interactive dashboard based on the engineered data."""
    return create_dashboard(user_request)

@tool
def create_dashboard(input_str: str = ""):
    """Generates interactive dashboard with stats."""
    context = "General Overview"
    if str(input_str).strip() in ["2", "1", "yes", "dashboard"]: context = "General"
    
    result = session.generate_dashboard(context)
    
    if "PATH:" in result:
        path = result.split("|STATS:")[0].replace("PATH:", "")
        stats = result.split("|STATS:")[1] if "|STATS:" in result else ""
        
        return f"""✅ **Dashboard Generated!**
        


**Key Data Insights:**
{stats}

(The interactive dashboard file `{path}` is ready. Please analyze these stats for the user.)"""
    
    return result