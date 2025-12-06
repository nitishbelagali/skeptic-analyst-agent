import polars as pl

# Define what our schema SHOULD look like (for Schema Drift detection)
EXPECTED_SCHEMA = {"date", "sales", "region"}

def check_structure(df: pl.DataFrame):
    issues = []
    
    # 1. Check Schema Drift (Missing/Extra columns)
    current_cols = set(df.columns)
    if not EXPECTED_SCHEMA.issubset(current_cols):
        missing = EXPECTED_SCHEMA - current_cols
        issues.append(f"SCHEMA DRIFT: Missing mandatory columns: {missing}")
    
    if len(current_cols) > len(EXPECTED_SCHEMA):
        extra = current_cols - EXPECTED_SCHEMA
        issues.append(f"SCHEMA DRIFT: Unexpected extra columns found: {extra}")

    # 2. Check Record Count (Empty file check)
    if df.height == 0:
        issues.append("FILE ERROR: Dataset is empty.")
        
    return issues

def check_integrity(df: pl.DataFrame):
    issues = []
    
    # 3. Check for Null Explosion (More than 50% nulls in any col)
    for col in df.columns:
        null_count = df[col].null_count()
        if null_count > 0:
            issues.append(f"NULLS: Column '{col}' has {null_count} missing values.")
            if null_count > (df.height * 0.5):
                issues.append(f"NULL EXPLOSION: Column '{col}' is >50% empty!")

    # 4. Duplicate Records (Exact row matches)
    if df.is_duplicated().sum() > 0:
        count = df.is_duplicated().sum()
        issues.append(f"DUPLICATES: Found {count} exact duplicate rows.")

    return issues

def check_validity(df: pl.DataFrame):
    issues = []
    
    # 5. Range Violations (Negative Sales)
    # We assume 'sales' must be numeric and positive
    if "sales" in df.columns and df["sales"].dtype in [pl.Float64, pl.Int64]:
        neg_sales = df.filter(pl.col("sales") < 0).height
        if neg_sales > 0:
            issues.append(f"RANGE VIOLATION: Found {neg_sales} rows with negative sales.")

        # 6. Outliers (Simple Z-Score or Threshold)
        # Let's say anything above 10,000 is suspicious for this business
        outliers = df.filter(pl.col("sales") > 10000).height
        if outliers > 0:
            issues.append(f"OUTLIERS: Found {outliers} sales records above 10,000.")

    # 7. Valid Regions (Business Rule)
    valid_regions = ["North", "South", "East", "West"]
    if "region" in df.columns:
        invalid_regions = df.filter(~pl.col("region").is_in(valid_regions)).height
        if invalid_regions > 0:
            issues.append(f"BUSINESS RULE: Found {invalid_regions} rows with invalid region names.")

    return issues

def run_all_checks(df: pl.DataFrame):
    """Runs all suites, compiles a report, and SAVES it to a file."""
    report = []
    report.extend(check_structure(df))
    report.extend(check_integrity(df))
    report.extend(check_validity(df))
    
    # Construct the final text
    if not report:
        final_output = "✅ AUDIT PASSED: Data looks clean across all checks."
    else:
        final_output = "❌ AUDIT FAILED:\n" + "\n".join([f"- {item}" for item in report])
        
    # --- NEW: Save to a temporary file for the PDF generator ---
    with open("temp_audit_log.txt", "w", encoding="utf-8") as f:
        f.write(final_output)
        
    return final_output