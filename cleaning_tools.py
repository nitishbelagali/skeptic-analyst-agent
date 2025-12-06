import polars as pl

def clean_dataset(df: pl.DataFrame) -> str:
    """
    Strict cleaning pipeline:
    1. Trims whitespace (to ensure "North " == "North").
    2. Filters Negatives AND Outliers (Sales > 10,000).
    3. Removes Nulls.
    4. Drops Duplicates (Performed LAST to catch everything).
    """
    initial_count = df.height
    
    # 1. Standardize Strings (Trim whitespace from ALL string columns)
    # This fixes the hidden "North " vs "North" mismatch
    df_clean = df.with_columns(pl.col(pl.String).str.strip_chars())
    
    # 2. Enforce Business Rules (Range & Outliers)
    # Remove negative sales AND sales greater than 10,000
    if "sales" in df_clean.columns:
         df_clean = df_clean.filter(
             (pl.col("sales") >= 0) & 
             (pl.col("sales") <= 10000)
         )

    # 3. Remove Nulls
    df_clean = df_clean.drop_nulls()
    
    # 4. Remove Duplicates
    # We do this LAST to ensure stripped strings match perfectly
    df_clean = df_clean.unique()

    final_count = df_clean.height
    removed_count = initial_count - final_count
    
    # 5. Save Cleaned Data
    output_file = "clean_sales_data.csv"
    df_clean.write_csv(output_file)
    
    return f"SUCCESS: Strict Clean Complete. Removed {removed_count} rows (incl. outliers > 10k). Saved '{output_file}'."