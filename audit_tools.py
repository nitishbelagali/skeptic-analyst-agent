import polars as pl

def check_structure(df):
    """Checks basic structure."""
    issues = []
    if df.width == 0:
        issues.append("CRITICAL: Dataset has no columns.")
    if df.height == 0:
        issues.append("CRITICAL: Dataset is empty (0 rows).")
    return issues

def check_integrity(df):
    """Universal checks for Nulls and Duplicates."""
    issues = []
    
    # 1. Check for Duplicates
    dup_count = df.is_duplicated().sum()
    if dup_count > 0:
        issues.append(f"DUPLICATES: Found {dup_count} exact duplicate rows.")

    # 2. Check for Nulls (All columns)
    for col in df.columns:
        null_count = df[col].null_count()
        if null_count > 0:
            pct = (null_count / df.height) * 100
            issues.append(f"NULLS: Column '{col}' has {null_count} missing values ({pct:.1f}%).")
            
    return issues

def check_validity(df):
    """Universal checks for Outliers and Negatives on ALL numeric columns."""
    issues = []
    
    # Whitelist: Columns that can legitimately be negative
    NEGATIVE_ALLOWED = ['loudness', 'db', 'decibel', 'temperature', 'profit', 'loss', 
                        'change', 'delta', 'diff', 'latitude', 'longitude']
    
    # Identify numeric columns automatically
    numeric_cols = [col for col in df.columns if df[col].dtype.is_numeric()]
    
    for col in numeric_cols:
        # 1. Check for Negatives (skip whitelisted columns)
        col_lower = col.lower()
        
        # Skip if column name suggests negatives are valid
        if not any(keyword in col_lower for keyword in NEGATIVE_ALLOWED):
            neg_count = df.filter(pl.col(col) < 0).height
            if neg_count > 0:
                issues.append(f"NEGATIVE VALUES: Column '{col}' has {neg_count} negative rows.")

        # 2. Check for Outliers (Universal IQR Method)
        if df.height > 10:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            
            if q1 is not None and q3 is not None:
                iqr = q3 - q1
                
                # Only check outliers if IQR > 0
                if iqr > 0:
                    upper_bound = q3 + (1.5 * iqr)
                    lower_bound = q1 - (1.5 * iqr)
                    
                    outlier_count = df.filter(
                        (pl.col(col) > upper_bound) | (pl.col(col) < lower_bound)
                    ).height
                    
                    if outlier_count > 0:
                        issues.append(f"OUTLIERS: Column '{col}' has {outlier_count} potential outliers.")

    return issues

    return issues

def run_all_checks(df):
    """Runs all audit checks and saves report to temp file."""
    report = []
    report.extend(check_structure(df))
    report.extend(check_integrity(df))
    report.extend(check_validity(df))

    if not report:
        final_output = " ✅  AUDIT PASSED (No obvious structural or statistical errors found)."
    else:
        final_output = " ❌  AUDIT FAILED:\n" + "\n".join([f"- {i}" for i in report])
    
    # ✅ FIX: Save to temp file for PDF generation
    try:
        with open("temp_audit_log.txt", "w", encoding="utf-8") as f:
            f.write(final_output)
    except Exception as e:
        print(f"Warning: Could not save audit log: {e}")
    
    return final_output