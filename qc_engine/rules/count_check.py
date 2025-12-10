"""
Count Check Rule
Compares row counts against expected values.
"""
from typing import Dict, Any
import pandas as pd

from ..base_rule import BaseRule, RuleResult


class CountCheckRule(BaseRule):
    """Check that row count matches expected value"""
    
    name = "Count Check"
    description = "Validates that the dataset has the expected number of rows"
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute count check.
        
        Config:
            expected_count: Expected row count (exact match)
            min_count: Minimum row count (optional)
            max_count: Maximum row count (optional)
            comparison: Type of comparison ('exact', 'min', 'max', 'range')
        """
        self.validate_config(config)
        
        expected_count = config.get('expected_count')
        min_count = config.get('min_count')
        max_count = config.get('max_count')
        comparison = config.get('comparison', 'exact')
        
        actual_count = len(df)
        passed = True
        details = {
            'actual_count': actual_count,
            'comparison_type': comparison
        }
        
        if comparison == 'exact':
            if expected_count is None:
                raise ValueError("expected_count is required for exact comparison")
            passed = actual_count == expected_count
            details['expected_count'] = expected_count
            details['difference'] = actual_count - expected_count
            message = f"Row count: {actual_count} (expected: {expected_count})"
            
        elif comparison == 'min':
            if min_count is None:
                raise ValueError("min_count is required for min comparison")
            passed = actual_count >= min_count
            details['min_count'] = min_count
            message = f"Row count: {actual_count} (minimum: {min_count})"
            
        elif comparison == 'max':
            if max_count is None:
                raise ValueError("max_count is required for max comparison")
            passed = actual_count <= max_count
            details['max_count'] = max_count
            message = f"Row count: {actual_count} (maximum: {max_count})"
            
        elif comparison == 'range':
            if min_count is None or max_count is None:
                raise ValueError("min_count and max_count are required for range comparison")
            passed = min_count <= actual_count <= max_count
            details['min_count'] = min_count
            details['max_count'] = max_count
            message = f"Row count: {actual_count} (range: {min_count}-{max_count})"
            
        else:
            raise ValueError(f"Unknown comparison type: {comparison}")
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            details=details,
            statistics={
                'actual_count': actual_count
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'comparison': {
                    'type': 'string',
                    'enum': ['exact', 'min', 'max', 'range'],
                    'default': 'exact',
                    'description': 'Type of count comparison'
                },
                'expected_count': {
                    'type': 'integer',
                    'description': 'Expected exact row count'
                },
                'min_count': {
                    'type': 'integer',
                    'description': 'Minimum row count'
                },
                'max_count': {
                    'type': 'integer',
                    'description': 'Maximum row count'
                }
            },
            'required': []
        }
