"""
QC Engine Module
Provides quality check rules and dataset comparison functionality.
"""
from .base_rule import BaseRule, RuleResult
from .comparator import DatasetComparator
from .rules import (
    NullCheckRule,
    DuplicateCheckRule,
    RangeCheckRule,
    DataTypeCheckRule,
    CountCheckRule,
    AggregationCheckRule,
    PatternCheckRule,
    UniquenessCheckRule,
    ValueSetCheckRule
)

# Rule registry for dynamic rule loading
RULE_REGISTRY = {
    'null_check': NullCheckRule,
    'duplicate_check': DuplicateCheckRule,
    'range_check': RangeCheckRule,
    'datatype_check': DataTypeCheckRule,
    'count_check': CountCheckRule,
    'aggregation_check': AggregationCheckRule,
    'pattern_check': PatternCheckRule,
    'uniqueness_check': UniquenessCheckRule,
    'value_set_check': ValueSetCheckRule
}


def get_available_rules():
    """Get list of available QC rules with their configurations"""
    rules = []
    for rule_id, rule_class in RULE_REGISTRY.items():
        rule = rule_class()
        rules.append({
            'id': rule_id,
            'name': rule.name,
            'description': rule.description,
            'config_schema': rule.get_config_schema()
        })
    return rules


def create_rule(rule_id: str) -> BaseRule:
    """Create a rule instance by ID"""
    if rule_id not in RULE_REGISTRY:
        raise ValueError(f"Unknown rule: {rule_id}")
    return RULE_REGISTRY[rule_id]()


__all__ = [
    'BaseRule', 'RuleResult', 'DatasetComparator',
    'NullCheckRule', 'DuplicateCheckRule', 'RangeCheckRule',
    'DataTypeCheckRule', 'CountCheckRule', 'AggregationCheckRule',
    'PatternCheckRule', 'UniquenessCheckRule', 'ValueSetCheckRule',
    'RULE_REGISTRY', 'get_available_rules', 'create_rule'
]
