"""
Input Validators
Validation utilities for user inputs.
"""
import re
from typing import List, Optional
import pandas as pd


def validate_columns(df: pd.DataFrame, columns: List[str]) -> tuple:
    """
    Validate that columns exist in DataFrame.
    
    Args:
        df: DataFrame to validate against
        columns: List of column names to check
        
    Returns:
        Tuple of (valid, missing_columns)
    """
    missing = [col for col in columns if col not in df.columns]
    return len(missing) == 0, missing


def validate_query(query: str) -> tuple:
    """
    Basic SQL query validation.
    
    Args:
        query: SQL query string
        
    Returns:
        Tuple of (valid, error_message)
    """
    if not query or not query.strip():
        return False, "Query cannot be empty"
    
    # Check for dangerous operations
    dangerous_patterns = [
        r'\bDROP\s+', r'\bDELETE\s+', r'\bTRUNCATE\s+',
        r'\bALTER\s+', r'\bCREATE\s+', r'\bINSERT\s+',
        r'\bUPDATE\s+', r'\bGRANT\s+', r'\bREVOKE\s+'
    ]
    
    query_upper = query.upper()
    for pattern in dangerous_patterns:
        if re.search(pattern, query_upper):
            return False, f"Dangerous operation detected: {pattern.split()[0]}"
    
    # Must start with SELECT
    if not query_upper.strip().startswith('SELECT'):
        return False, "Only SELECT queries are allowed"
    
    return True, None


def validate_file_extension(filename: str, allowed: Optional[List[str]] = None) -> bool:
    """
    Validate file extension.
    
    Args:
        filename: Name of the file
        allowed: List of allowed extensions (with dot)
        
    Returns:
        True if valid, False otherwise
    """
    if allowed is None:
        allowed = ['.csv', '.xlsx', '.xls']
    
    import os
    ext = os.path.splitext(filename)[1].lower()
    return ext in allowed


def validate_numeric(value: str) -> tuple:
    """
    Validate that a string can be parsed as a number.
    
    Args:
        value: String to validate
        
    Returns:
        Tuple of (valid, parsed_value or error_message)
    """
    try:
        parsed = float(value)
        return True, parsed
    except (ValueError, TypeError):
        return False, f"'{value}' is not a valid number"


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename for safe storage.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename
    """
    # Remove path separators and null bytes
    filename = filename.replace('/', '_').replace('\\', '_').replace('\0', '')
    
    # Remove other potentially dangerous characters
    filename = re.sub(r'[<>:"|?*]', '_', filename)
    
    # Limit length
    if len(filename) > 200:
        name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
        filename = name[:196] + '.' + ext if ext else name[:200]
    
    return filename
