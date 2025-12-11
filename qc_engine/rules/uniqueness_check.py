"""
Uniqueness Check Rule
Validates that column values are unique (no duplicates).
Common use cases: IDs, email addresses, account numbers.
"""
from typing import Dict, Any
import pandas as pd

from ..base_rule import BaseRule, RuleResult


class UniquenessCheckRule(BaseRule):
    """Validate that column values are unique"""
    
    name = "Uniqueness Check"
    description = "Validates that a column contains only unique values (no duplicates)"
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute uniqueness check.
        
        Config:
            column: Column to check for uniqueness
            case_sensitive: Whether to treat values as case-sensitive (default: True)
            ignore_nulls: Whether to ignore NULL values in uniqueness check (default: True)
        """
        self.validate_config(config)
        
        column = config['column']
        case_sensitive = config.get('case_sensitive', True)
        ignore_nulls = config.get('ignore_nulls', True)
        
        self.validate_columns(df, [column])
        
        # Prepare data
        data = df[column].copy()
        
        # Handle case sensitivity
        if not case_sensitive and data.dtype == 'object':
            # Create a comparison series with lowercase values
            compare_data = data.str.lower() if hasattr(data, 'str') else data
        else:
            compare_data = data
        
        # Find duplicates
        if ignore_nulls:
            # Exclude nulls from duplicate check
            non_null_mask = data.notna()
            duplicated_mask = compare_data[non_null_mask].duplicated(keep=False)
            # Map back to full dataframe
            full_duplicated_mask = pd.Series(False, index=df.index)
            full_duplicated_mask[non_null_mask] = duplicated_mask
        else:
            full_duplicated_mask = compare_data.duplicated(keep=False)
        
        # Get duplicate values and their counts
        duplicate_rows = df[full_duplicated_mask]
        
        if len(duplicate_rows) > 0:
            # Count occurrences of each duplicate value
            if not case_sensitive and data.dtype == 'object':
                value_counts = compare_data[full_duplicated_mask].value_counts()
            else:
                value_counts = data[full_duplicated_mask].value_counts()
            
            # Create violations list
            violations = []
            for value, count in value_counts.items():
                if pd.isna(value):
                    display_value = 'NULL'
                else:
                    display_value = str(value)
                violations.append({
                    'value': display_value,
                    'occurrences': int(count)
                })
            
            # Get sample rows for each duplicate value
            duplicate_details = []
            for value in value_counts.index[:20]:  # Limit to top 20 duplicate values
                if pd.isna(value):
                    mask = data.isna()
                else:
                    if not case_sensitive and data.dtype == 'object':
                        mask = compare_data == value
                    else:
                        mask = data == value
                
                rows = df[mask].index.tolist()
                duplicate_details.append({
                    'value': 'NULL' if pd.isna(value) else str(value),
                    'count': int(len(rows)),
                    'row_numbers': [int(r) + 1 for r in rows[:10]]  # Show first 10 row numbers
                })
        else:
            violations = []
            duplicate_details = []
        
        total_rows = len(df)
        unique_count = data.nunique(dropna=ignore_nulls)
        duplicate_count = len(duplicate_rows)
        passed = duplicate_count == 0
        
        # Build message
        if passed:
            message = f"All {total_rows} values in '{column}' are unique"
        else:
            num_duplicate_values = len(violations)
            message = f"{duplicate_count} duplicate rows found across {num_duplicate_values} distinct value(s) in '{column}'"
        
        # Create failed_rows for display
        failed_rows_display = []
        for detail in duplicate_details[:50]:  # Limit display
            failed_rows_display.append({
                column: detail['value'],
                'occurrences': detail['count'],
                'sample_rows': ', '.join(map(str, detail['row_numbers'][:5]))
            })
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            failed_rows=failed_rows_display,
            failed_row_count=duplicate_count,
            details={
                'column': column,
                'violations': violations,
                'duplicate_details': duplicate_details,
                'total_duplicate_rows': duplicate_count,
                'unique_values': unique_count
            },
            statistics={
                'total_rows': total_rows,
                'unique_values': unique_count,
                'duplicate_rows': duplicate_count,
                'uniqueness_rate': round(unique_count / total_rows * 100, 2) if total_rows > 0 else 0
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'column': {
                    'type': 'string',
                    'description': 'Column to check for uniqueness'
                },
                'case_sensitive': {
                    'type': 'boolean',
                    'default': True,
                    'description': 'Treat values as case-sensitive'
                },
                'ignore_nulls': {
                    'type': 'boolean',
                    'default': True,
                    'description': 'Ignore NULL values in uniqueness check'
                }
            },
            'required': ['column']
        }
