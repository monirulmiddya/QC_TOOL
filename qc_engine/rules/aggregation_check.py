"""
Aggregation Check Rule
Validates aggregations (sum, avg, min, max, count) with optional grouping.
Supports multiple columns with different aggregation functions.
"""
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np

from ..base_rule import BaseRule, RuleResult


class AggregationCheckRule(BaseRule):
    """Check aggregation values against expected values"""
    
    name = "Aggregation Check"
    description = "Validates aggregation results - supports multiple columns with different functions"
    
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
            aggregations: List of {column, function} pairs
            group_by: Optional columns to group by
            expected_value: Optional expected aggregation result (for single agg)
            tolerance: Acceptable tolerance for comparison (default: 0)
            
            Legacy support:
            column: Single column to aggregate (if aggregations not provided)
            aggregation: Single aggregation function (if aggregations not provided)
        """
        self.validate_config(config)
        
        # Support new multi-aggregation format
        aggregations = config.get('aggregations', [])
        
        # Fallback to legacy single column/aggregation format
        if not aggregations:
            column = config.get('column')
            aggregation = config.get('aggregation', 'sum')
            if column:
                aggregations = [{'column': column, 'function': aggregation}]
        
        if not aggregations:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                message="No aggregations configured",
                details={'error': 'No column or aggregations specified'}
            )
        
        group_by = config.get('group_by', [])
        expected_value = config.get('expected_value')
        tolerance = config.get('tolerance', 0)
        tolerance_type = config.get('tolerance_type', 'absolute')
        
        # Validate columns
        all_columns = [a['column'] for a in aggregations]
        self.validate_columns(df, all_columns)
        if group_by:
            self.validate_columns(df, group_by)
        
        # Perform all aggregations
        results = []
        all_passed = True
        grouped_data = {}  # Store grouped data by key
        
        for agg_config in aggregations:
            column = agg_config['column']
            agg_func_name = agg_config.get('function', 'sum').lower()
            
            if agg_func_name not in self.AGGREGATION_FUNCTIONS:
                results.append({
                    'column': column,
                    'function': agg_func_name,
                    'error': f"Unknown aggregation: {agg_func_name}"
                })
                continue
            
            agg_func = self.AGGREGATION_FUNCTIONS[agg_func_name]
            agg_label = f"{agg_func_name.upper()}({column})"
            
            # Perform aggregation
            if group_by:
                grouped = df.groupby(group_by)[column].agg(agg_func).reset_index()
                grouped.columns = list(group_by) + [agg_label]
                
                # Merge into grouped_data
                for _, row in grouped.iterrows():
                    key = tuple(row[col] for col in group_by)
                    if key not in grouped_data:
                        grouped_data[key] = {col: row[col] for col in group_by}
                    val = row[agg_label]
                    grouped_data[key][agg_label] = float(val) if pd.notna(val) else None
                
                overall_value = df[column].agg(agg_func)
            else:
                overall_value = df[column].agg(agg_func)
            
            # Convert numpy types to Python types
            overall_value = float(overall_value) if pd.notna(overall_value) else None
            
            result_item = {
                'column': column,
                'function': agg_func_name.upper(),
                'value': overall_value
            }
            
            # Check against expected value if single aggregation
            if expected_value is not None and len(aggregations) == 1:
                if tolerance_type == 'percentage':
                    tolerance_amount = abs(expected_value * (tolerance / 100))
                else:
                    tolerance_amount = tolerance
                
                if overall_value is not None:
                    difference = abs(overall_value - expected_value)
                    passed = difference <= tolerance_amount
                    result_item['expected'] = expected_value
                    result_item['difference'] = round(difference, 4)
                    result_item['passed'] = passed
                    if not passed:
                        all_passed = False
            
            results.append(result_item)
        
        # Convert grouped_data dict to list for table display
        grouped_table = list(grouped_data.values()) if grouped_data else []
        
        # Build message
        if len(aggregations) == 1:
            r = results[0]
            message = f"{r['function']}({r['column']}): {r['value']}"
            if group_by:
                message += f" (grouped by {group_by})"
        else:
            summary = ", ".join([f"{r['function']}({r['column']})={r['value']}" for r in results])
            message = f"Aggregations: {summary}"
            if group_by:
                message += f" (grouped by {group_by})"
        
        return RuleResult(
            rule_name=self.name,
            passed=all_passed,
            message=message,
            details={
                'aggregations': results,
                'group_by': group_by,
                'grouped_table': grouped_table
            },
            statistics={
                'aggregation_results': results,
                'grouped_row_count': len(grouped_table)
            }
        )
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'properties': {
                'aggregations': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'column': {'type': 'string'},
                            'function': {'type': 'string', 'enum': list(self.AGGREGATION_FUNCTIONS.keys())}
                        }
                    },
                    'description': 'List of column + function pairs to aggregate'
                },
                'group_by': {
                    'type': 'array',
                    'items': {'type': 'string'},
                    'description': 'Columns to group by before aggregation'
                },
                'expected_value': {
                    'type': 'number',
                    'description': 'Expected aggregation result (for single aggregation)'
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
                    'description': 'Type of tolerance'
                },
                # Legacy support
                'column': {
                    'type': 'string',
                    'description': 'Column to aggregate (legacy)'
                },
                'aggregation': {
                    'type': 'string',
                    'enum': list(self.AGGREGATION_FUNCTIONS.keys()),
                    'description': 'Aggregation function (legacy)'
                }
            },
            'required': []
        }
