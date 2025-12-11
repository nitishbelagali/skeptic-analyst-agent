import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import pandas as pd
import os
from datetime import datetime

class VisualizationSession:
    def __init__(self, db_path="warehouse.db"):
        self.db_path = db_path
        # Modern color scheme
        self.colors = {
            'primary': '#2E86AB',
            'secondary': '#A23B72',
            'success': '#06A77D',
            'warning': '#F18F01',
            'background': '#F8F9FA'
        }

    def generate_dashboard(self):
        """Generates interactive HTML dashboard"""
        if not os.path.exists(self.db_path):
            return "Error: Database not found."

        conn = None
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
            
            fact_table = "fact_table"
            dim_tables = [t for t in tables if t.startswith("dim_")]
            
            if fact_table not in tables:
                return "Error: Fact table not found."
            
            fact_cols_info = conn.execute(f"DESCRIBE {fact_table}").fetchall()
            fact_col_names = [c[0] for c in fact_cols_info]
            
            # Create subplots
            fig = make_subplots(
                rows=3, cols=3,
                subplot_titles=('KPI Overview', '', '', 
                               'Activity Over Time', '', 'Top Categories',
                               'Distribution', 'Correlation Matrix', 'Key Metrics'),
                specs=[
                    [{"type": "indicator", "colspan": 3}, None, None],
                    [{"type": "scatter", "colspan": 2}, None, {"type": "bar"}],
                    [{"type": "histogram"}, {"type": "heatmap"}, {"type": "table"}]
                ],
                vertical_spacing=0.12,
                horizontal_spacing=0.1
            )
            
            row_count = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()[0]
            
            # 1. KPI Indicators (Top row)
            self._add_kpi_cards(fig, conn, fact_table, dim_tables, fact_cols_info, row_count)
            
            # 2. Time Series (Middle-left, wide)
            self._add_time_series(fig, conn, fact_table, dim_tables, fact_col_names, row=2, col=1)
            
            # 3. Category Breakdown (Middle-right)
            self._add_categories(fig, conn, fact_table, dim_tables, fact_col_names, row=2, col=3)
            
            # 4. Distribution (Bottom-left)
            self._add_distribution(fig, conn, fact_table, fact_cols_info, row=3, col=1)
            
            # 5. Correlation (Bottom-middle)
            self._add_correlation(fig, conn, fact_table, fact_cols_info, row=3, col=2)
            
            # 6. Summary Table (Bottom-right)
            self._add_summary_table(fig, conn, fact_table, dim_tables, row=3, col=3)
            
            # Layout Fixes
            fig.update_layout(
                title_text=f"<b>Interactive Data Analytics Dashboard</b><br><sub>Analysis of {row_count:,} records | Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</sub>",
                title_font_size=24,
                showlegend=True,
                # FIX: Move legend to top-left to avoid overlapping with colorbar
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                height=1200,
                paper_bgcolor=self.colors['background'],
                plot_bgcolor='white',
                font=dict(family="Arial, sans-serif", size=12)
            )
            
            output_path = "dashboard_report.html"
            fig.write_html(output_path, config={'displayModeBar': False})
            
            return f"Interactive dashboard generated: {output_path}"
            
        except Exception as e:
            return f"Dashboard failed: {e}"
        finally:
            if conn: conn.close()

    def _add_kpi_cards(self, fig, conn, fact_table, dim_tables, fact_cols_info, row_count):
        """Add KPI indicator cards"""
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=row_count,
                title={"text": f"<b>Total Records</b><br><span style='font-size:0.8em'>Across {len(dim_tables)} dimensions</span>"},
                domain={'x': [0, 1], 'y': [0, 1]}
            ),
            row=1, col=1
        )

    def _add_time_series(self, fig, conn, fact_table, dim_tables, fact_cols, row, col):
        """Add interactive time series"""
        date_dim = next((d for d in dim_tables if "date" in d.lower() or "time" in d.lower()), None)
        if not date_dim: return
        
        try:
            col_name = date_dim.replace("dim_", "")
            fk_name = f"{col_name}_id"
            query = f"SELECT d.{col_name} as dt, COUNT(*) as cnt FROM {fact_table} f JOIN {date_dim} d ON f.{fk_name} = d.{col_name}_id GROUP BY 1 ORDER BY 1"
            df = conn.execute(query).fetchdf()
            df['dt'] = pd.to_datetime(df['dt'])
            
            fig.add_trace(
                go.Scatter(x=df['dt'], y=df['cnt'], mode='lines+markers', name='Activity',
                           line=dict(color=self.colors['primary'], width=3),
                           marker=dict(size=8), fill='tozeroy'),
                row=row, col=col
            )
            fig.update_xaxes(title_text="Date", row=row, col=col)
            fig.update_yaxes(title_text="Count", row=row, col=col)
        except: pass

    def _add_categories(self, fig, conn, fact_table, dim_tables, fact_cols, row, col):
        """Add interactive category chart"""
        date_dim = next((d for d in dim_tables if "date" in d.lower() or "time" in d.lower()), None)
        cat_dim = next((d for d in dim_tables if d != date_dim), None)
        if not cat_dim: return
        
        try:
            col_name = cat_dim.replace("dim_", "")
            fk_name = f"{col_name}_id"
            query = f"SELECT d.{col_name} as cat, COUNT(*) as cnt FROM {fact_table} f JOIN {cat_dim} d ON f.{fk_name} = d.{col_name}_id GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
            df = conn.execute(query).fetchdf()
            
            fig.add_trace(
                go.Bar(y=df['cat'], x=df['cnt'], orientation='h', name=col_name.title(),
                       marker=dict(color=df['cnt'], colorscale='Viridis', showscale=False)),
                row=row, col=col
            )
            fig.update_xaxes(title_text="Count", row=row, col=col)
            fig.update_yaxes(title_text=col_name.title(), row=row, col=col)
        except: pass

    def _add_distribution(self, fig, conn, fact_table, fact_cols_info, row, col):
        """Add distribution histogram"""
        numeric_col = next((c[0] for c in fact_cols_info if c[0] not in ['fact_id'] and not c[0].endswith('_id') and any(x in c[1].upper() for x in ['INT', 'FLOAT', 'DOUBLE'])), None)
        if not numeric_col: return
        
        try:
            df = conn.execute(f"SELECT {numeric_col} FROM {fact_table}").fetchdf()
            fig.add_trace(
                go.Histogram(x=df[numeric_col], name=numeric_col, marker=dict(color=self.colors['success'])),
                row=row, col=col
            )
            fig.update_xaxes(title_text=numeric_col.title(), row=row, col=col)
            fig.update_yaxes(title_text="Frequency", row=row, col=col)
        except: pass

    def _add_correlation(self, fig, conn, fact_table, fact_cols_info, row, col):
        """Add correlation heatmap"""
        numeric_cols = [c[0] for c in fact_cols_info if c[0] not in ['fact_id'] and not c[0].endswith('_id') and any(x in c[1].upper() for x in ['INT', 'FLOAT', 'DOUBLE'])]
        if len(numeric_cols) < 2: return
        
        try:
            df = conn.execute(f"SELECT {', '.join(numeric_cols)} FROM {fact_table}").fetchdf()
            corr = df.corr()
            fig.add_trace(
                go.Heatmap(z=corr.values, x=corr.columns, y=corr.columns, colorscale='RdBu', zmid=0,
                           # FIX: Make colorbar horizontal and thin to save space
                           colorbar=dict(title="Corr", orientation='h', thickness=15, y=-0.3)),
                row=row, col=col
            )
        except: pass

    def _add_summary_table(self, fig, conn, fact_table, dim_tables, row, col):
        """Add summary statistics table"""
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()[0]
            fact_cols = conn.execute(f"DESCRIBE {fact_table}").fetchall()
            numeric_col = next((c[0] for c in fact_cols if c[0] not in ['fact_id'] and not c[0].endswith('_id') and any(x in c[1].upper() for x in ['INT', 'FLOAT', 'DOUBLE'])), None)
            
            stats_data = [['Total Records', f'{row_count:,}'], ['Dimensions', str(len(dim_tables))]]
            if numeric_col:
                stats = conn.execute(f"SELECT MIN({numeric_col}), MAX({numeric_col}), AVG({numeric_col}) FROM {fact_table}").fetchone()
                stats_data.extend([[f'Min {numeric_col}', f'{stats[0]:,.2f}'], [f'Max {numeric_col}', f'{stats[1]:,.2f}'], [f'Avg {numeric_col}', f'{stats[2]:,.2f}']])
            
            fig.add_trace(
                go.Table(
                    header=dict(values=['<b>Metric</b>', '<b>Value</b>'], fill_color=self.colors['primary'], font=dict(color='white')),
                    cells=dict(values=[[r[0] for r in stats_data], [r[1] for r in stats_data]], fill_color='white')
                ),
                row=row, col=col
            )
        except: pass

session = VisualizationSession()