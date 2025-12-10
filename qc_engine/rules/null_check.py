"""
Null Check Rule
Checks for null/missing values in specified columns.
"""
from typing import Dict, Any
import pandas as pd

from ..base_rule import BaseRule, RuleResult


class NullCheckRule(BaseRule):
    """Check for null/missing values in specified columns"""
    
    name = "Null Check"
    description = "Validates that specified columns do not contain null or missing values"
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute null check on specified columns.
        
        Config:
            columns: List of column names to check
            threshold: Optional percentage threshold (0-100) for acceptable nulls
        """
        self.validate_config(config)
        columns = config.get('columns', [])
        threshold = config.get('threshold', 0)  # Default: no nulls allowed
        
        if not columns:
            columns = df.columns.tolist()
        
        self.validate_columns(df, columns)
        
        # Check for nulls
        null_counts = {}
        failed_rows_list = []
        total_rows = len(df)
        
        for col in columns:
            null_count = df[col].isna().sum()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            null_counts[col] = {
                'null_count': int(null_count),
                'null_percentage': round(null_pct, 2)
            }
            
            if null_count > 0:
                failed_rows = df[df[col].isna()].copy()
                failed_rows['_failed_column'] = col
                failed_rows_list.append(failed_rows)
        
        # Determine pass/fail based on threshold
        violations = {col: data for col, data in null_counts.items() 
                     if data['null_percentage'] > threshold}
        
        passed = len(violations) == 0
        
        if passed:
            message = f"All {len(columns)} columns pass null check"
        else:
            message = f"{len(violations)} column(s) exceed null threshold of {threshold}%"
        
        # Combine failed rows
        failed_df = pd.concat(failed_rows_list, ignore_index=True) if failed_rows_list else pd.DataFrame()
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            details={
                'columns_checked': columns,
                'null_counts': null_counts,
                'violations': violations,
                'threshold': threshold
            },
            failed_rows=failed_df,
            statistics={
                'total_rows': total_rows,
                'total_null_cells': sum(d['null_count'] for d in null_counts.values())
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'columns': {
                    'type': 'array',
                    'items': {'type': 'string'},
                    'description': 'Columns to check for nulls (empty = all columns)'
                },
                'threshold': {
                    'type': 'number',
                    'minimum': 0,
                    'maximum': 100,
                    'default': 0,
                    'description': 'Maximum acceptable null percentage (0-100)'
                }
            },
            'required': []
        }
