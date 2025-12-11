import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import pandas as pd
import os
from datetime import datetime

class VisualizationSession:
    def __init__(self, db_path="warehouse.db"):
        self.db_path = db_path
        
        # Professional color palette
        self.colors = {
            'primary': '#334155',    # Slate 700 (Text/Headers)
            'secondary': '#6366f1',  # Indigo 500 (Main Charts)
            'accent': '#0ea5e9',     # Sky 500 (Secondary Charts)
            'success': '#10b981',    # Emerald 500 (Positive/Growth)
            'background': '#f8fafc', # Slate 50 (Paper Background)
            'grid': '#e2e8f0'        # Slate 200 (Grid lines)
        }

    def generate_dashboard(self, context=""):
        """
        Generates interactive HTML dashboard.
        
        Args:
            context: User's original question (e.g., "Which genre has most movies?")
                     Used to customize chart selection.
        """
        if not os.path.exists(self.db_path):
            return "❌ Error: Database not found. Run engineering pipeline first."

        conn = None
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            
            # Get database metadata
            tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
            fact_table = "fact_table"
            dim_tables = [t for t in tables if t.startswith("dim_")]
            
            if fact_table not in tables:
                return "❌ Error: Fact table not found. Run transformation first."
            
            fact_cols_info = conn.execute(f"DESCRIBE {fact_table}").fetchall()
            row_count = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()[0]
            
            # Determine dashboard mode based on context
            mode = "General Overview"
            if context:
                context_lower = context.lower()
                if any(word in context_lower for word in ["genre", "category", "type"]):
                    mode = "Category Analysis"
                elif any(word in context_lower for word in ["trend", "time", "over time", "timeline"]):
                    mode = "Trend Analysis"
                elif any(word in context_lower for word in ["country", "region", "location"]):
                    mode = "Geographic Analysis"
            
            # Create layout (2x2 grid)
            fig = make_subplots(
                rows=2, cols=2,
                column_widths=[0.65, 0.35],
                row_heights=[0.5, 0.5],
                subplot_titles=(
                    f'Primary Analysis: {mode}', 
                    'Category Breakdown', 
                    'Distribution Analysis', 
                    'Key Metrics'
                ),
                specs=[
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "table"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.08
            )
            
            # Add charts
            self._add_primary_chart(fig, conn, fact_table, dim_tables, context, row=1, col=1)
            self._add_category_chart(fig, conn, fact_table, dim_tables, row=1, col=2)
            self._add_distribution_chart(fig, conn, fact_table, fact_cols_info, row=2, col=1)
            self._add_summary_table(fig, conn, fact_table, dim_tables, row=2, col=2)
            
            # Global styling
            fig.update_layout(
                title=dict(
                    text=f"<b>{mode} Dashboard</b><br><span style='font-size:14px; color:#64748b'>Analysis of {row_count:,} records | Generated: {datetime.now().strftime('%B %d, %Y')}</span>",
                    font=dict(size=24, color=self.colors['primary']),
                    x=0.02,
                    y=0.95
                ),
                template="plotly_white",
                height=950,
                showlegend=False,
                paper_bgcolor=self.colors['background'],
                plot_bgcolor='white',
                font=dict(family="Inter, Arial, sans-serif", color=self.colors['primary']),
                margin=dict(t=100, l=50, r=50, b=50)
            )
            
            # Save as HTML
            output_path = "dashboard_report.html"
            fig.write_html(output_path, config={'displayModeBar': False})
            
            conn.close()
            return output_path
            
        except Exception as e:
            if conn:
                conn.close()
            return f"❌ Dashboard generation failed: {e}"

    def _add_primary_chart(self, fig, conn, fact_table, dim_tables, context, row, col):
        """Context-aware primary chart (switches between line/bar based on question)."""
        context_lower = context.lower() if context else ""
        
        # Strategy A: If user asks about specific category (e.g., "genre")
        if "genre" in context_lower:
            genre_dim = next((d for d in dim_tables if "genre" in d.lower()), None)
            if genre_dim:
                col_name = genre_dim.replace("dim_", "")
                query = f"""
                    SELECT d.{col_name} as x, COUNT(*) as y 
                    FROM {fact_table} f
                    JOIN {genre_dim} d ON f.{col_name}_id = d.{col_name}_id
                    GROUP BY 1
                    ORDER BY 2 DESC
                    LIMIT 10
                """
                self._render_bar(fig, conn, query, "Top Genres", row, col, self.colors['secondary'])
                return
        
        # Strategy B: If user asks about time trends
        date_dim = next((d for d in dim_tables if "date" in d.lower() or "time" in d.lower()), None)
        
        if ("trend" in context_lower or "time" in context_lower) and date_dim:
            col_name = date_dim.replace("dim_", "")
            query = f"""
                SELECT d.{col_name} as x, COUNT(*) as y 
                FROM {fact_table} f
                JOIN {date_dim} d ON f.{col_name}_id = d.{col_name}_id
                GROUP BY 1
                ORDER BY 1
            """
            self._render_line(fig, conn, query, "Activity Over Time", row, col)
            return
        
        # Strategy C: Default fallback (time series if available, else top category)
        if date_dim:
            col_name = date_dim.replace("dim_", "")
            query = f"""
                SELECT d.{col_name} as x, COUNT(*) as y 
                FROM {fact_table} f
                JOIN {date_dim} d ON f.{col_name}_id = d.{col_name}_id
                GROUP BY 1
                ORDER BY 1
            """
            self._render_line(fig, conn, query, "Activity Timeline", row, col)
        elif dim_tables:
            # Fallback to first dimension
            dim = dim_tables[0]
            col_name = dim.replace("dim_", "")
            query = f"""
                SELECT d.{col_name} as x, COUNT(*) as y 
                FROM {fact_table} f
                JOIN {dim} d ON f.{col_name}_id = d.{col_name}_id
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 10
            """
            self._render_bar(fig, conn, query, f"Top {col_name.title()}s", row, col, self.colors['secondary'])

    def _render_bar(self, fig, conn, query, title, row, col, color):
        """Renders horizontal bar chart."""
        try:
            df = conn.execute(query).fetchdf()
            
            if df.empty:
                return
            
            fig.add_trace(
                go.Bar(
                    x=df['y'],
                    y=df['x'],
                    orientation='h',
                    marker=dict(color=color, cornerradius=5),
                    hovertemplate='<b>%{y}</b><br>Count: %{x}<extra></extra>',
                    name=title
                ),
                row=row, col=col
            )
            
            fig.update_xaxes(showgrid=True, gridcolor=self.colors['grid'], row=row, col=col)
            fig.update_yaxes(showgrid=False, autorange="reversed", row=row, col=col)
            
        except Exception as e:
            print(f"Bar chart error: {e}")

    def _render_line(self, fig, conn, query, title, row, col):
        """Renders line chart with area fill."""
        try:
            df = conn.execute(query).fetchdf()
            
            if df.empty:
                return
            
            # Convert to datetime
            df['x'] = pd.to_datetime(df['x'], errors='coerce')
            df = df.dropna(subset=['x'])
            
            if df.empty:
                return
            
            fig.add_trace(
                go.Scatter(
                    x=df['x'],
                    y=df['y'],
                    mode='lines+markers',
                    line=dict(color=self.colors['secondary'], width=3),
                    marker=dict(
                        size=6,
                        color='white',
                        line=dict(width=2, color=self.colors['secondary'])
                    ),
                    fill='tozeroy',
                    fillcolor='rgba(99, 102, 241, 0.1)',
                    hovertemplate='<b>%{x|%Y-%m-%d}</b><br>Count: %{y}<extra></extra>',
                    name=title
                ),
                row=row, col=col
            )
            
            fig.update_xaxes(showgrid=True, gridcolor=self.colors['grid'], row=row, col=col)
            fig.update_yaxes(showgrid=True, gridcolor=self.colors['grid'], row=row, col=col)
            
        except Exception as e:
            print(f"Line chart error: {e}")

    def _add_category_chart(self, fig, conn, fact_table, dim_tables, row, col):
        """Secondary category breakdown (finds non-date dimension)."""
        # Find a dimension that isn't date/time or genre
        target = next(
            (d for d in dim_tables 
             if "genre" not in d.lower() 
             and "date" not in d.lower() 
             and "time" not in d.lower()),
            dim_tables[0] if dim_tables else None
        )
        
        if not target:
            return
        
        try:
            col_name = target.replace("dim_", "")
            query = f"""
                SELECT d.{col_name} as x, COUNT(*) as y
                FROM {fact_table} f
                JOIN {target} d ON f.{col_name}_id = d.{col_name}_id
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 8
            """
            
            df = conn.execute(query).fetchdf()
            
            if not df.empty:
                fig.add_trace(
                    go.Bar(
                        x=df['y'],
                        y=df['x'],
                        orientation='h',
                        marker=dict(color=self.colors['accent'], cornerradius=3),
                        hovertemplate='<b>%{y}</b><br>Count: %{x}<extra></extra>'
                    ),
                    row=row, col=col
                )
                
                fig.update_yaxes(autorange="reversed", row=row, col=col)
                
        except Exception as e:
            print(f"Category chart error: {e}")

    def _add_distribution_chart(self, fig, conn, fact_table, fact_cols_info, row, col):
        """Distribution histogram of first numeric measure."""
        # Find first numeric non-ID column
        numeric_col = None
        for col_info in fact_cols_info:
            col_name = col_info[0]
            col_type = col_info[1]
            
            if col_name not in ['fact_id'] and not col_name.endswith('_id'):
                if any(x in col_type.upper() for x in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL']):
                    numeric_col = col_name
                    break
        
        if not numeric_col:
            return
        
        try:
            df = conn.execute(f"SELECT {numeric_col} FROM {fact_table} WHERE {numeric_col} IS NOT NULL").fetchdf()
            
            if df.empty:
                return
            
            fig.add_trace(
                go.Histogram(
                    x=df[numeric_col],
                    marker=dict(color=self.colors['success'], opacity=0.8),
                    nbinsx=20,
                    hovertemplate='<b>Range:</b> %{x}<br><b>Count:</b> %{y}<extra></extra>'
                ),
                row=row, col=col
            )
            
            fig.update_xaxes(title_text=numeric_col.replace('_', ' ').title(), row=row, col=col)
            fig.update_yaxes(title_text="Frequency", row=row, col=col)
            fig.update_layout(bargap=0.1)
            
        except Exception as e:
            print(f"Distribution chart error: {e}")

    def _add_summary_table(self, fig, conn, fact_table, dim_tables, row, col):
        """Summary statistics table."""
        try:
            # Get basic stats
            row_count = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()[0]
            
            # Get date range if available
            date_dim = next((d for d in dim_tables if "date" in d.lower()), None)
            date_range = "N/A"
            
            if date_dim:
                col_name = date_dim.replace("dim_", "")
                try:
                    dates = conn.execute(f"SELECT MIN({col_name}), MAX({col_name}) FROM {date_dim}").fetchone()
                    date_range = f"{dates[0]} to {dates[1]}"
                except:
                    pass
            
            # Build table data
            header_vals = ['<b>Metric</b>', '<b>Value</b>']
            cell_vals = [
                ['Total Records', 'Dimensions', 'Date Range'],
                [f"{row_count:,}", str(len(dim_tables)), date_range]
            ]
            
            fig.add_trace(
                go.Table(
                    header=dict(
                        values=header_vals,
                        fill_color=self.colors['primary'],
                        font=dict(color='white', size=12),
                        align='left'
                    ),
                    cells=dict(
                        values=cell_vals,
                        fill_color='white',
                        font=dict(color=self.colors['primary'], size=12),
                        align='left',
                        height=30
                    )
                ),
                row=row, col=col
            )
            
        except Exception as e:
            print(f"Summary table error: {e}")

# Global session instance
session = VisualizationSession()