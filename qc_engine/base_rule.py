"""
Base Rule Abstract Class
Defines the interface for all QC rules.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import pandas as pd


@dataclass
class RuleResult:
    """Result of a QC rule execution"""
    rule_name: str
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    failed_rows: Optional[pd.DataFrame] = None
    statistics: Dict[str, Any] = field(default_factory=dict)
    
    @staticmethod
    def _convert_to_serializable(obj):
        """Convert numpy types to native Python types for JSON serialization"""
        import numpy as np
        
        if isinstance(obj, dict):
            return {RuleResult._convert_to_serializable(k): RuleResult._convert_to_serializable(v) 
                    for k, v in obj.items()}
        elif isinstance(obj, list):
            return [RuleResult._convert_to_serializable(item) for item in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        else:
            return obj
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = {
            'rule_name': self.rule_name,
            'passed': bool(self.passed),  # Ensure native bool
            'message': self.message,
            'details': self._convert_to_serializable(self.details),
            'statistics': self._convert_to_serializable(self.statistics)
        }
        
        if self.failed_rows is not None and not self.failed_rows.empty:
            # Limit failed rows for display and convert to serializable format
            failed_data = self.failed_rows.head(100).to_dict('records')
            result['failed_rows'] = self._convert_to_serializable(failed_data)
            result['failed_row_count'] = len(self.failed_rows)
        
        return result


class BaseRule(ABC):
    """Abstract base class for QC rules"""
    
    name: str = "Base Rule"
    description: str = "Base QC rule"
    
    @abstractmethod
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> RuleResult:
        """
        Execute the QC rule on a DataFrame.
        
        Args:
            df: The DataFrame to check
            config: Rule configuration parameters
            
        Returns:
            RuleResult with pass/fail status and details
        """
        pass
    
    @abstractmethod
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get the configuration schema for this rule.
        
        Returns:
            Dictionary describing the configuration parameters
        """
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate configuration against schema"""
        schema = self.get_config_schema()
        required = schema.get('required', [])
        
        for field in required:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")
        
        return True
    
    def validate_columns(self, df: pd.DataFrame, columns: List[str]) -> None:
        """Validate that specified columns exist in DataFrame"""
        missing = [col for col in columns if col not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in data: {missing}")
