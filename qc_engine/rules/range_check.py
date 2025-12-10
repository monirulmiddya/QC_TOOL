"""
Range Check Rule
Validates that numeric values fall within specified min/max range.
"""
from typing import Dict, Any
import pandas as pd
import numpy as np

from ..base_rule import BaseRule, RuleResult


class RangeCheckRule(BaseRule):
    """Check that numeric values are within specified range"""
    
    name = "Range Check"
    description = "Validates that numeric column values fall within specified min/max bounds"
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute range check on specified column.
        
        Config:
            column: Column name to check
            min_value: Minimum allowed value (optional)
            max_value: Maximum allowed value (optional)
            inclusive: Whether bounds are inclusive (default: True)
        """
        self.validate_config(config)
        column = config['column']
        min_value = config.get('min_value')
        max_value = config.get('max_value')
        inclusive = config.get('inclusive', True)
        
        self.validate_columns(df, [column])
        
        # Convert to numeric if needed
        series = pd.to_numeric(df[column], errors='coerce')
        
        # Build mask for out-of-range values
        out_of_range_mask = pd.Series([False] * len(df))
        
        if min_value is not None:
            if inclusive:
                out_of_range_mask |= (series < min_value)
            else:
                out_of_range_mask |= (series <= min_value)
        
        if max_value is not None:
            if inclusive:
                out_of_range_mask |= (series > max_value)
            else:
                out_of_range_mask |= (series >= max_value)
        
        # Get failed rows
        failed_rows = df[out_of_range_mask].copy()
        violation_count = out_of_range_mask.sum()
        
        # Calculate statistics
        valid_values = series.dropna()
        stats = {
            'min': float(valid_values.min()) if len(valid_values) > 0 else None,
            'max': float(valid_values.max()) if len(valid_values) > 0 else None,
            'mean': float(valid_values.mean()) if len(valid_values) > 0 else None,
            'median': float(valid_values.median()) if len(valid_values) > 0 else None
        }
        
        passed = violation_count == 0
        total_rows = len(df)
        
        bounds_desc = []
        if min_value is not None:
            bounds_desc.append(f"min={min_value}")
        if max_value is not None:
            bounds_desc.append(f"max={max_value}")
        bounds_str = ", ".join(bounds_desc)
        
        if passed:
            message = f"All values in '{column}' are within range ({bounds_str})"
        else:
            message = f"{violation_count} values in '{column}' are out of range ({bounds_str})"
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            details={
                'column': column,
                'min_value': min_value,
                'max_value': max_value,
                'inclusive': inclusive,
                'actual_range': stats
            },
            failed_rows=failed_rows,
            statistics={
                'total_rows': total_rows,
                'violation_count': int(violation_count),
                'violation_percentage': round(violation_count / total_rows * 100, 2) if total_rows > 0 else 0,
                **stats
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'column': {
                    'type': 'string',
                    'description': 'Column to check for value range'
                },
                'min_value': {
                    'type': 'number',
                    'description': 'Minimum allowed value'
                },
                'max_value': {
                    'type': 'number',
                    'description': 'Maximum allowed value'
                },
                'inclusive': {
                    'type': 'boolean',
                    'default': True,
                    'description': 'Whether bounds are inclusive'
                }
            },
            'required': ['column']
        }
