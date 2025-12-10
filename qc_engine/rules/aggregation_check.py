"""
Aggregation Check Rule
Validates aggregations (sum, avg, min, max, count) with optional grouping.
"""
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np

from ..base_rule import BaseRule, RuleResult


class AggregationCheckRule(BaseRule):
    """Check aggregation values against expected values"""
    
    name = "Aggregation Check"
    description = "Validates that aggregation results match expected values"
    
    AGGREGATION_FUNCTIONS = {
        'sum': 'sum',
        'avg': 'mean',
        'mean': 'mean',
        'min': 'min',
        'max': 'max',
        'count': 'count',
        'count_distinct': 'nunique',
        'std': 'std',
        'var': 'var'
    }
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute aggregation check.
        
        Config:
            column: Column to aggregate
            aggregation: Aggregation function (sum, avg, min, max, count, count_distinct)
            expected_value: Expected aggregation result
            tolerance: Acceptable tolerance for comparison (default: 0)
            tolerance_type: 'absolute' or 'percentage' (default: absolute)
            group_by: Optional columns to group by
        """
        self.validate_config(config)
        
        column = config['column']
        aggregation = config['aggregation'].lower()
        expected_value = config.get('expected_value')
        tolerance = config.get('tolerance', 0)
        tolerance_type = config.get('tolerance_type', 'absolute')
        group_by = config.get('group_by', [])
        
        self.validate_columns(df, [column])
        if group_by:
            self.validate_columns(df, group_by)
        
        if aggregation not in self.AGGREGATION_FUNCTIONS:
            raise ValueError(f"Unknown aggregation: {aggregation}. Supported: {list(self.AGGREGATION_FUNCTIONS.keys())}")
        
        agg_func = self.AGGREGATION_FUNCTIONS[aggregation]
        
        # Perform aggregation
        if group_by:
            grouped = df.groupby(group_by)[column].agg(agg_func)
            actual_values = grouped.to_dict()
            overall_value = df[column].agg(agg_func)
        else:
            overall_value = df[column].agg(agg_func)
            actual_values = {'_overall': overall_value}
        
        # Convert numpy types to Python types
        overall_value = float(overall_value) if pd.notna(overall_value) else None
        
        # Check against expected value
        passed = True
        comparison_details = {}
        
        if expected_value is not None:
            if tolerance_type == 'percentage':
                tolerance_amount = abs(expected_value * (tolerance / 100))
            else:
                tolerance_amount = tolerance
            
            if overall_value is not None:
                difference = abs(overall_value - expected_value)
                passed = difference <= tolerance_amount
                comparison_details = {
                    'expected': expected_value,
                    'actual': overall_value,
                    'difference': round(difference, 4),
                    'tolerance': tolerance,
                    'tolerance_type': tolerance_type,
                    'within_tolerance': passed
                }
        
        # Build message
        if group_by:
            message = f"{aggregation.upper()}({column}) by {group_by}: {overall_value}"
        else:
            message = f"{aggregation.upper()}({column}): {overall_value}"
        
        if expected_value is not None and not passed:
            message += f" (expected: {expected_value}, diff: {abs(overall_value - expected_value):.4f})"
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            details={
                'column': column,
                'aggregation': aggregation,
                'group_by': group_by,
                'comparison': comparison_details
            },
            statistics={
                'aggregated_value': overall_value,
                'grouped_values': {str(k): float(v) if pd.notna(v) else None 
                                  for k, v in actual_values.items()} if group_by else None
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'column': {
                    'type': 'string',
                    'description': 'Column to aggregate'
                },
                'aggregation': {
                    'type': 'string',
                    'enum': list(self.AGGREGATION_FUNCTIONS.keys()),
                    'description': 'Aggregation function to apply'
                },
                'expected_value': {
                    'type': 'number',
                    'description': 'Expected aggregation result'
                },
                'tolerance': {
                    'type': 'number',
                    'default': 0,
                    'description': 'Acceptable tolerance for comparison'
                },
                'tolerance_type': {
                    'type': 'string',
                    'enum': ['absolute', 'percentage'],
                    'default': 'absolute',
                    'description': 'Type of tolerance (absolute or percentage)'
                },
                'group_by': {
                    'type': 'array',
                    'items': {'type': 'string'},
                    'description': 'Columns to group by before aggregation'
                }
            },
            'required': ['column', 'aggregation']
        }
