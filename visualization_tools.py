import polars as pl
import cleaning_tools
from langchain_core.tools import tool

def visualize_data_tool(input_str: str = ""):
    """
    Generates a Power BI-style interactive dashboard using Plotly.
    Includes SMART COLUMN DETECTION and HEATMAPS.
    """
    # 1. Check for Data
    df = cleaning_tools.session.current_df
    if df is None: return "❌ No data to visualize."

    # 2. Safe Imports
    try:
        import pandas as pd
        import plotly.express as px
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError as e:
        return f"❌ Library Missing: {e}. Please run: `pip install plotly pandas pyarrow`"

    try:
        pdf = df.to_pandas()
        
        # --- 🧠 SMART COLUMN DETECTION ---
        num_cols = []
        cat_cols = []
        date_cols = []

        # Step A: Classify based on types
        for col in df.columns:
            dtype = df[col].dtype
            if dtype.is_numeric(): num_cols.append(col)
            elif dtype == pl.Date or isinstance(dtype, pl.Datetime): date_cols.append(col)
            elif dtype == pl.Utf8 or dtype == pl.Categorical: cat_cols.append(col)
            else:
                try: 
                    if dtype == pl.String: cat_cols.append(col)
                except: pass

        # Step B: Select the BEST Category Column
        clean_cats = [c for c in cat_cols if not any(x in c.lower() for x in ['id', 'code', 'uuid', 'guid', 'pk'])]
        priority_names = ['category', 'segment', 'region', 'department', 'group', 'type', 'class']
        selected_cat = next((c for c in clean_cats if any(p in c.lower() for p in priority_names)), None)
        
        if not selected_cat and clean_cats:
            selected_cat = sorted(clean_cats, key=lambda c: pdf[c].nunique())[0]

        final_cat_col = selected_cat if selected_cat else (cat_cols[0] if cat_cols else None)
        final_num_col = num_cols[0] if num_cols else None
        final_date_col = date_cols[0] if date_cols else None

        # --- HEATMAP LOGIC ---
        # Only show heatmap if we have at least 2 numeric columns to compare
        show_heatmap = len(num_cols) >= 2
        
        # --- DASHBOARD LAYOUT (5 Rows if Heatmap, 4 if not) ---
        row_heights = [0.1, 0.25, 0.25, 0.25, 0.15] if show_heatmap else [0.15, 0.35, 0.35, 0.15]
        specs_list = [
            [{"type": "domain"}, {"type": "domain"}],      # Row 1: KPI
            [{"colspan": 2, "type": "xy"}, None],           # Row 2: Trend
            [{"type": "xy"}, {"type": "domain"}],           # Row 3: Bar + Pie
        ]
        
        if show_heatmap:
             specs_list.append([{"colspan": 2, "type": "xy"}, None]) # Row 4: Heatmap
        
        specs_list.append([{"colspan": 2, "type": "table"}, None])   # Row 5 (or 4): Table

        subplot_titles = [
            "Total Metric", "Record Count", 
            "Trend Analysis", 
            f"Top {final_cat_col}s" if final_cat_col else "Categories", 
            f"Composition", 
        ]
        if show_heatmap: subplot_titles.append("Correlation Heatmap")
        subplot_titles.append("Data Preview")

        fig = make_subplots(
            rows=len(specs_list), cols=2,
            specs=specs_list,
            subplot_titles=tuple(subplot_titles),
            vertical_spacing=0.08,
            row_heights=row_heights
        )
        
        # 1. KPI CARDS
        primary_metric = final_num_col if final_num_col else "Count"
        total_val = pdf[primary_metric].sum() if final_num_col else len(pdf)
        
        fig.add_trace(go.Indicator(
            mode="number",
            value=total_val,
            title={"text": f"Total {primary_metric}"},
            number={'prefix': "$" if 'sales' in primary_metric.lower() or 'revenue' in primary_metric.lower() else "", 'font': {'size': 40}},
        ), row=1, col=1)
        
        fig.add_trace(go.Indicator(
            mode="number",
            value=len(pdf),
            title={"text": "Total Records"},
            number={'font': {'size': 40}}
        ), row=1, col=2)
        
        # 2. MAIN TREND
        if final_date_col and final_num_col:
            trend_df = pdf.sort_values(by=final_date_col)
            fig.add_trace(go.Scatter(
                x=trend_df[final_date_col], y=trend_df[final_num_col],
                mode='lines', name=f"{final_num_col} Trend",
                line=dict(color='#636EFA', width=3), fill='tozeroy'
            ), row=2, col=1)
        elif final_num_col:
            fig.add_trace(go.Histogram(
                x=pdf[final_num_col], name="Distribution",
                marker_color='#EF553B', opacity=0.8
            ), row=2, col=1)

        # 3. CATEGORICAL BREAKDOWN
        if final_cat_col:
            top_cats = pdf[final_cat_col].value_counts().head(10)
            fig.add_trace(go.Bar(
                x=top_cats.index, y=top_cats.values,
                name=final_cat_col, marker_color='#00CC96'
            ), row=3, col=1)
            
            fig.add_trace(go.Pie(
                labels=top_cats.index, values=top_cats.values,
                name=final_cat_col, hole=.5,
                marker=dict(colors=px.colors.qualitative.Prism)
            ), row=3, col=2)
        
        # 4. HEATMAP (Conditional)
        next_row = 4
        if show_heatmap:
            corr_matrix = pdf[num_cols].corr()
            fig.add_trace(go.Heatmap(
                z=corr_matrix.values,
                x=corr_matrix.columns,
                y=corr_matrix.index,
                colorscale='Viridis',
                showscale=True
            ), row=4, col=1)
            next_row = 5
        
        # 5. DATA TABLE
        preview_df = pdf.head(5)
        fig.add_trace(go.Table(
            header=dict(values=list(preview_df.columns), fill_color='#334155', font=dict(color='white'), align='left'),
            cells=dict(values=[preview_df[k].tolist() for k in preview_df.columns], fill_color='#1e293b', font=dict(color='lightgrey'), align='left')
        ), row=next_row, col=1)

        fig.update_layout(
            height=1200 if show_heatmap else 1000, 
            showlegend=False, template="plotly_dark",
            paper_bgcolor='#1e293b', plot_bgcolor='#0f172a',
            font=dict(color="#e2e8f0", family="Arial"),
            margin=dict(t=50, l=20, r=20, b=20)
        )
        
        fig.write_html("dashboard_report.html")
        
        # --- GENERATE DETAILED VISUAL STORY STATS ---
        stats = f"Analyzed {len(pdf)} records. "
        if final_cat_col:
            top_cat = pdf[final_cat_col].mode()[0]
            top_count = pdf[final_cat_col].value_counts().iloc[0]
            pct = (top_count / len(pdf)) * 100
            stats += f"**Key Insight:** The dominant '{final_cat_col}' is **{top_cat}** ({pct:.1f}% of total). "
        
        if show_heatmap:
            stats += "A correlation heatmap was generated to show relationships between numerical variables."

        return f"✅ Dashboard Generated (dashboard_report.html).\nSTATS: {stats}"

    except Exception as e:
        return f"❌ Visualization Error: {str(e)}"

# --- TOOL WRAPPERS ---
@tool
def create_dashboard(input_str: str = ""):
    """Generates an interactive HTML dashboard with automated charts."""
    return visualize_data_tool(input_str)