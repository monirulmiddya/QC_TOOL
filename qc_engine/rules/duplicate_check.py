"""
Duplicate Check Rule
Checks for duplicate rows based on specified columns.
"""
from typing import Dict, Any
import pandas as pd

from ..base_rule import BaseRule, RuleResult


class DuplicateCheckRule(BaseRule):
    """Check for duplicate rows in the dataset"""
    
    name = "Duplicate Check"
    description = "Identifies duplicate rows based on specified key columns"
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute duplicate check.
        
        Config:
            columns: List of columns to use as keys (empty = all columns)
            keep: Which duplicates to mark ('first', 'last', 'none')
        """
        self.validate_config(config)
        columns = config.get('columns', [])
        keep = config.get('keep', 'first')
        
        if not columns:
            columns = df.columns.tolist()
        else:
            self.validate_columns(df, columns)
        
        # Find duplicates
        duplicated_mask = df.duplicated(subset=columns, keep=False if keep == 'none' else keep)
        duplicate_count = int(duplicated_mask.sum())
        
        # Get duplicate rows
        duplicate_rows = df[duplicated_mask].copy()
        
        # Group duplicates for analysis
        if not duplicate_rows.empty:
            duplicate_groups = df[df.duplicated(subset=columns, keep=False)].groupby(columns).size()
            # Convert to JSON-serializable format
            group_sizes = {int(k): int(v) for k, v in duplicate_groups.value_counts().items()}
        else:
            group_sizes = {}
        
        passed = duplicate_count == 0
        total_rows = len(df)
        unique_count = total_rows - duplicate_count
        
        if passed:
            message = f"No duplicates found in {total_rows} rows"
        else:
            message = f"Found {duplicate_count} duplicate rows ({round(duplicate_count/total_rows*100, 2)}%)"
        
        return RuleResult(
            rule_name=self.name,
            passed=bool(passed),
            message=message,
            details={
                'columns_checked': columns,
                'keep_strategy': keep,
                'group_sizes': group_sizes
            },
            failed_rows=duplicate_rows,
            statistics={
                'total_rows': int(total_rows),
                'unique_rows': int(unique_count),
                'duplicate_rows': int(duplicate_count),
                'duplicate_percentage': float(round(duplicate_count / total_rows * 100, 2)) if total_rows > 0 else 0.0
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'columns': {
                    'type': 'array',
                    'items': {'type': 'string'},
                    'description': 'Columns to check for duplicates (empty = all columns)'
                },
                'keep': {
                    'type': 'string',
                    'enum': ['first', 'last', 'none'],
                    'default': 'first',
                    'description': 'Which duplicate to keep when identifying duplicates'
                }
            },
            'required': []
        }
