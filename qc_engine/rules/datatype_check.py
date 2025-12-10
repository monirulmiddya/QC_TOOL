"""
Data Type Check Rule
Validates that column values match expected data types.
"""
from typing import Dict, Any
import pandas as pd
import numpy as np

from ..base_rule import BaseRule, RuleResult


class DataTypeCheckRule(BaseRule):
    """Check that column values match expected data types"""
    
    name = "Data Type Check"
    description = "Validates that column values can be parsed as the expected data type"
    
    # Mapping of type names to validation functions
    TYPE_VALIDATORS = {
        'integer': lambda x: pd.to_numeric(x, errors='coerce').notna() & (pd.to_numeric(x, errors='coerce') % 1 == 0),
        'float': lambda x: pd.to_numeric(x, errors='coerce').notna(),
        'numeric': lambda x: pd.to_numeric(x, errors='coerce').notna(),
        'string': lambda x: x.apply(lambda v: isinstance(v, str) if pd.notna(v) else True),
        'date': lambda x: pd.to_datetime(x, errors='coerce').notna(),
        'datetime': lambda x: pd.to_datetime(x, errors='coerce').notna(),
        'boolean': lambda x: x.isin([True, False, 'true', 'false', 'True', 'False', 1, 0, '1', '0']),
        'email': lambda x: x.str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', na=False),
    }
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute data type check.
        
        Config:
            column: Column name to check
            expected_type: Expected data type
            allow_nulls: Whether to allow null values (default: True)
        """
        self.validate_config(config)
        column = config['column']
        expected_type = config['expected_type'].lower()
        allow_nulls = config.get('allow_nulls', True)
        
        self.validate_columns(df, [column])
        
        if expected_type not in self.TYPE_VALIDATORS:
            raise ValueError(f"Unknown type: {expected_type}. Supported: {list(self.TYPE_VALIDATORS.keys())}")
        
        series = df[column]
        validator = self.TYPE_VALIDATORS[expected_type]
        
        # Get valid mask
        valid_mask = validator(series)
        
        # Handle nulls
        if allow_nulls:
            valid_mask = valid_mask | series.isna()
        
        # Get failed rows
        failed_mask = ~valid_mask
        failed_rows = df[failed_mask].copy()
        violation_count = failed_mask.sum()
        
        # Get sample of invalid values
        invalid_values = series[failed_mask].head(10).tolist()
        
        passed = violation_count == 0
        total_rows = len(df)
        
        if passed:
            message = f"All values in '{column}' match type '{expected_type}'"
        else:
            message = f"{violation_count} values in '{column}' do not match type '{expected_type}'"
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            details={
                'column': column,
                'expected_type': expected_type,
                'allow_nulls': allow_nulls,
                'current_dtype': str(df[column].dtype),
                'sample_invalid_values': invalid_values
            },
            failed_rows=failed_rows,
            statistics={
                'total_rows': total_rows,
                'valid_count': int((~failed_mask).sum()),
                'invalid_count': int(violation_count),
                'invalid_percentage': round(violation_count / total_rows * 100, 2) if total_rows > 0 else 0
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'column': {
                    'type': 'string',
                    'description': 'Column to check for data type'
                },
                'expected_type': {
                    'type': 'string',
                    'enum': list(self.TYPE_VALIDATORS.keys()),
                    'description': 'Expected data type'
                },
                'allow_nulls': {
                    'type': 'boolean',
                    'default': True,
                    'description': 'Whether null values are allowed'
                }
            },
            'required': ['column', 'expected_type']
        }
