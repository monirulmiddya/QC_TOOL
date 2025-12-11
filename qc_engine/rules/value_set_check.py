"""
Value Set Check Rule  
Validates that column values are from an allowed set of values.
Common use cases: status codes, categories, enums.
"""
from typing import Dict, Any, List
import pandas as pd

from ..base_rule import BaseRule, RuleResult


class ValueSetCheckRule(BaseRule):
    """Validate column values are from an allowed set"""
    
    name = "Value Set Check"
    description = "Validates that column values are from a specified allowed set"
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute value set check.
        
        Config:
            column: Column to check
            allowed_values: List of allowed values
            case_sensitive: Whether matching is case-sensitive (default: True)
            allow_null: Whether to allow NULL values (default: False)
        """
        self.validate_config(config)
        
        column = config['column']
        allowed_values = config['allowed_values']
        case_sensitive = config.get('case_sensitive', True)
        allow_null = config.get('allow_null', False)
        
        self.validate_columns(df, [column])
        
        # Validate allowed_values is a list
        if not isinstance(allowed_values, list):
            return RuleResult(
                rule_name=self.name,
                passed=False,
                message="allowed_values must be a list",
                details={'error': 'Invalid configuration'}
            )
        
        if len(allowed_values) == 0:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                message="allowed_values cannot be empty",
                details={'error': 'Invalid configuration'}
            )
        
        # Prepare allowed set
        if not case_sensitive:
            # Convert to lowercase for comparison
            allowed_set = {str(v).lower() if v is not None else None for v in allowed_values}
        else:
            allowed_set = set(allowed_values)
        
        # Check each value
        violations = []
        null_count = 0
        invalid_value_counts = {}
        
        for idx, value in df[column].items():
            if pd.isna(value):
                null_count += 1
                if not allow_null:
                    violations.append({
                        'row': int(idx) + 1,
                        'value': None,
                        'reason': 'NULL value not allowed'
                    })
                    invalid_value_counts['NULL'] = invalid_value_counts.get('NULL', 0) + 1
            else:
                # Check if value is in allowed set
                if not case_sensitive:
                    check_value = str(value).lower()
                else:
                    check_value = value
                
                if check_value not in allowed_set:
                    violations.append({
                        'row': int(idx) + 1,
                        'value': value,
                        'reason': f'Not in allowed set: {allowed_values}'
                    })
                    display_value = str(value)
                    invalid_value_counts[display_value] = invalid_value_counts.get(display_value, 0) + 1
        
        total_rows = len(df)
        failed_count = len(violations)
        passed = failed_count == 0
        
        # Create frequency distribution of invalid values
        invalid_freq = [
            {'value': val, 'count': count}
            for val, count in sorted(invalid_value_counts.items(), key=lambda x: x[1], reverse=True)
        ]
        
        # Build message
        if passed:
            message = f"All {total_rows} values in '{column}' are from allowed set"
        else:
            unique_invalid = len(invalid_value_counts)
            message = f"{failed_count} of {total_rows} values in '{column}' are not in allowed set ({unique_invalid} unique invalid value(s))"
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            failed_rows=[{
                column: v['value'] if v['value'] is not None else 'NULL',
                'row_number': v['row'],
                'reason': v['reason']
            } for v in violations[:100]],  # Limit to 100 for display
            failed_row_count=failed_count,
            details={
                'column': column,
                'allowed_values': allowed_values,
                'violations': violations[:100],
                'invalid_value_frequency': invalid_freq,
                'total_violations': failed_count,
                'null_count': null_count
            },
            statistics={
                'total_rows': total_rows,
                'valid_count': total_rows - failed_count,
                'invalid_count': failed_count,
                'null_count': null_count,
                'unique_invalid_values': len(invalid_value_counts),
                'compliance_rate': round((total_rows - failed_count) / total_rows * 100, 2) if total_rows > 0 else 0
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'column': {
                    'type': 'string',
                    'description': 'Column to validate'
                },
                'allowed_values': {
                    'type': 'array',
                    'items': {'type': 'string'},
                    'description': 'List of allowed values'
                },
                'case_sensitive': {
                    'type': 'boolean',
                    'default': True,
                    'description': 'Case-sensitive matching'
                },
                'allow_null': {
                    'type': 'boolean',
                    'default': False,
                    'description': 'Allow NULL values'
                }
            },
            'required': ['column', 'allowed_values']
        }
