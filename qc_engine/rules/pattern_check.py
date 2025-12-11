"""
Pattern Check Rule
Validates column values match a regex pattern.
Common use cases: email, phone, date formats, IDs, SKUs.
"""
from typing import Dict, Any
import pandas as pd
import re

from ..base_rule import BaseRule, RuleResult


class PatternCheckRule(BaseRule):
    """Validate column values match a regex pattern"""
    
    name = "Pattern Check"
    description = "Validates that column values match a specified regex pattern"
    
    # Common patterns library
    COMMON_PATTERNS = {
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'phone_us': r'^\d{3}-\d{3}-\d{4}$',
        'phone_intl': r'^\+\d{1,3}-\d{3,14}$',
        'date_iso': r'^\d{4}-\d{2}-\d{2}$',
        'date_us': r'^\d{2}/\d{2}/\d{4}$',
        'zip_us': r'^\d{5}(-\d{4})?$',
        'ssn_us': r'^\d{3}-\d{2}-\d{4}$',
        'url': r'^https?://[^\s]+$',
        'ipv4': r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$',
        'alphanumeric': r'^[a-zA-Z0-9]+$',
        'alpha_only': r'^[a-zA-Z]+$',
        'numeric_only': r'^\d+$'
    }
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute pattern check.
        
        Config:
            column: Column to check
            pattern: Regex pattern or common pattern name
            case_sensitive: Whether to use case-sensitive matching (default: True)
            allow_null: Whether to allow null values (default: False)
        """
        self.validate_config(config)
        
        column = config['column']
        pattern_input = config['pattern']
        case_sensitive = config.get('case_sensitive', True)
        allow_null = config.get('allow_null', False)
        
        self.validate_columns(df, [column])
        
        # Get pattern (either from common patterns or use custom)
        if pattern_input in self.COMMON_PATTERNS:
            pattern = self.COMMON_PATTERNS[pattern_input]
            pattern_display = f"{pattern_input} ({pattern})"
        else:
            pattern = pattern_input
            pattern_display = pattern
        
        # Compile regex
        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            regex = re.compile(pattern, flags)
        except re.error as e:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                message=f"Invalid regex pattern: {e}",
                details={'error': str(e)}
            )
        
        # Check each value
        violations = []
        null_count = 0
        
        for idx, value in df[column].items():
            if pd.isna(value):
                null_count += 1
                if not allow_null:
                    violations.append({
                        'row': int(idx) + 1,
                        'value': None,
                        'reason': 'Null value not allowed'
                    })
            else:
                str_value = str(value)
                if not regex.match(str_value):
                    violations.append({
                        'row': int(idx) + 1,
                        'value': str_value,
                        'reason': f'Does not match pattern: {pattern_display}'
                    })
        
        total_rows = len(df)
        failed_count = len(violations)
        passed = failed_count == 0
        
        # Build message
        if passed:
            message = f"All {total_rows} values in '{column}' match pattern: {pattern_display}"
        else:
            message = f"{failed_count} of {total_rows} values in '{column}' do not match pattern"
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            failed_rows=[{column: v['value'], 'row_number': v['row'], 'reason': v['reason']} 
                         for v in violations[:100]],  # Limit to 100 for display
            failed_row_count=failed_count,
            details={
                'column': column,
                'pattern': pattern_display,
                'violations': violations[:100],  # Detailed violations
                'total_violations': failed_count,
                'null_count': null_count
            },
            statistics={
                'total_rows': total_rows,
                'valid_count': total_rows - failed_count,
                'invalid_count': failed_count,
                'null_count': null_count,
                'pass_rate': round((total_rows - failed_count) / total_rows * 100, 2) if total_rows > 0 else 0
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'column': {
                    'type': 'string',
                    'description': 'Column to validate'
                },
                'pattern': {
                    'type': 'string',
                    'description': f'Regex pattern or common pattern name: {", ".join(self.COMMON_PATTERNS.keys())}'
                },
                'case_sensitive': {
                    'type': 'boolean',
                    'default': True,
                    'description': 'Case-sensitive matching'
                },
                'allow_null': {
                    'type': 'boolean',
                    'default': False,
                    'description': 'Allow null values'
                }
            },
            'required': ['column', 'pattern']
        }
