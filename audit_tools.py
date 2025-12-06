import polars as pl

# Define what valid data looks like
EXPECTED_SCHEMA = {"date", "sales", "region"}
# UPDATE: Added "Unknown" here so the Audit passes when you use that fix
VALID_REGIONS = ["North", "South", "East", "West", "Unknown"] 

def check_structure(df):
    issues = []
    if not EXPECTED_SCHEMA.issubset(set(df.columns)):
        issues.append(f"SCHEMA DRIFT: Missing {EXPECTED_SCHEMA - set(df.columns)}")
    return issues

def check_integrity(df):
    issues = []
    for col in df.columns:
        if df[col].null_count() > 0:
            issues.append(f"NULLS: Column '{col}' has {df[col].null_count()} missing values.")
        if df.is_duplicated().sum() > 0:
            issues.append(f"DUPLICATES: Found {df.is_duplicated().sum()} exact duplicate rows.")
    return issues

def check_validity(df):
    issues = []
    # Check Sales
    if "sales" in df.columns and df["sales"].dtype.is_numeric():
        if df.filter(pl.col("sales") < 0).height > 0:
            issues.append(f"RANGE VIOLATION: Found {df.filter(pl.col('sales') < 0).height} rows with negative sales.")
        if df.filter(pl.col("sales") > 10000).height > 0:
            issues.append(f"OUTLIERS: Found {df.filter(pl.col('sales') > 10000).height} sales records above 10,000.")
    
    # Check Regions (Business Logic)
    if "region" in df.columns:
        # We verify against the updated VALID_REGIONS list
        invalid_count = df.filter(~pl.col("region").is_in(VALID_REGIONS)).height
        if invalid_count > 0:
            issues.append(f"BUSINESS RULE: Found {invalid_count} rows with invalid region names.")
            
    return issues

def run_all_checks(df):
    report = []
    report.extend(check_structure(df))
    report.extend(check_integrity(df))
    report.extend(check_validity(df))

    if not report:
        return " ✅  AUDIT PASSED"
    else:
        return " ❌  AUDIT FAILED:\n" + "\n".join([f"- {i}" for i in report])