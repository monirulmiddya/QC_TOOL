"""
Dataset Comparator
Compares two datasets and identifies differences.
"""
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import numpy as np
from dataclasses import dataclass, field


@dataclass
class ComparisonResult:
    """Result of dataset comparison"""
    match: bool
    message: str
    summary: Dict[str, Any] = field(default_factory=dict)
    column_differences: Dict[str, Any] = field(default_factory=dict)
    row_differences: Dict[str, Any] = field(default_factory=dict)
    statistics: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'match': self.match,
            'message': self.message,
            'summary': self.summary,
            'column_differences': self.column_differences,
            'row_differences': self.row_differences,
            'statistics': self.statistics
        }


class DatasetComparator:
    """Compare two datasets and identify differences"""
    
    def compare(self, df1: pd.DataFrame, df2: pd.DataFrame,
                key_columns: List[str] = None,
                compare_columns: List[str] = None,
                tolerance: float = 0,
                ignore_case: bool = False,
                ignore_whitespace: bool = False) -> ComparisonResult:
        """
        Compare two DataFrames and return differences.
        
        Args:
            df1: First DataFrame (source)
            df2: Second DataFrame (target)
            key_columns: Columns to use as keys for row matching
            compare_columns: Specific columns to compare (None = all)
            tolerance: Numeric tolerance for floating point comparisons
            ignore_case: Ignore case in string comparisons
            ignore_whitespace: Strip whitespace in string comparisons
            
        Returns:
            ComparisonResult with detailed differences
        """
        # Schema comparison
        schema_match, schema_diff = self._compare_schemas(df1, df2)
        
        if not schema_match and compare_columns is None:
            # Find common columns
            common_cols = list(set(df1.columns) & set(df2.columns))
            if not common_cols:
                return ComparisonResult(
                    match=False,
                    message="No common columns between datasets",
                    summary={'schema_match': False},
                    column_differences=schema_diff
                )
            compare_columns = common_cols
        
        if compare_columns is None:
            compare_columns = list(df1.columns)
        
        # Shape comparison
        shape_match = len(df1) == len(df2)
        
        # Row-level comparison
        if key_columns:
            row_diff = self._compare_with_keys(
                df1, df2, key_columns, compare_columns, tolerance, 
                ignore_case, ignore_whitespace
            )
        else:
            row_diff = self._compare_positional(
                df1, df2, compare_columns, tolerance,
                ignore_case, ignore_whitespace
            )
        
        # Calculate statistics
        stats = self._calculate_statistics(df1, df2, compare_columns)
        
        # Determine overall match
        overall_match = (
            schema_match and 
            shape_match and 
            row_diff['total_differences'] == 0
        )
        
        if overall_match:
            message = "Datasets are identical"
        else:
            issues = []
            if not schema_match:
                issues.append("schema differences")
            if not shape_match:
                issues.append(f"row count mismatch ({len(df1)} vs {len(df2)})")
            if row_diff['total_differences'] > 0:
                issues.append(f"{row_diff['total_differences']} value differences")
            message = "Differences found: " + ", ".join(issues)
        
        return ComparisonResult(
            match=overall_match,
            message=message,
            summary={
                'schema_match': schema_match,
                'shape_match': shape_match,
                'rows_compared': min(len(df1), len(df2)),
                'total_differences': row_diff['total_differences']
            },
            column_differences=schema_diff,
            row_differences=row_diff,
            statistics=stats
        )
    
    def _compare_schemas(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[bool, Dict]:
        """Compare DataFrame schemas"""
        cols1 = set(df1.columns)
        cols2 = set(df2.columns)
        
        only_in_1 = list(cols1 - cols2)
        only_in_2 = list(cols2 - cols1)
        common = list(cols1 & cols2)
        
        # Check data types for common columns
        type_mismatches = {}
        for col in common:
            if df1[col].dtype != df2[col].dtype:
                type_mismatches[col] = {
                    'source_type': str(df1[col].dtype),
                    'target_type': str(df2[col].dtype)
                }
        
        schema_match = len(only_in_1) == 0 and len(only_in_2) == 0 and len(type_mismatches) == 0
        
        return schema_match, {
            'only_in_source': only_in_1,
            'only_in_target': only_in_2,
            'common_columns': common,
            'type_mismatches': type_mismatches
        }
    
    def _compare_positional(self, df1: pd.DataFrame, df2: pd.DataFrame,
                           compare_columns: List[str], tolerance: float,
                           ignore_case: bool, ignore_whitespace: bool) -> Dict:
        """Compare DataFrames row by row positionally"""
        min_rows = min(len(df1), len(df2))
        differences = []
        
        for col in compare_columns:
            if col not in df1.columns or col not in df2.columns:
                continue
            
            s1 = df1[col].iloc[:min_rows].reset_index(drop=True)
            s2 = df2[col].iloc[:min_rows].reset_index(drop=True)
            
            # Apply transformations
            if ignore_case and s1.dtype == object:
                s1 = s1.str.lower()
                s2 = s2.str.lower()
            if ignore_whitespace and s1.dtype == object:
                s1 = s1.str.strip()
                s2 = s2.str.strip()
            
            # Compare
            if np.issubdtype(s1.dtype, np.number) and tolerance > 0:
                diff_mask = ~(np.abs(s1 - s2) <= tolerance) & ~(s1.isna() & s2.isna())
            else:
                diff_mask = (s1 != s2) & ~(s1.isna() & s2.isna())
            
            diff_indices = diff_mask[diff_mask].index.tolist()
            
            for idx in diff_indices[:100]:  # Limit to first 100 differences per column
                differences.append({
                    'row_index': int(idx),
                    'column': col,
                    'source_value': str(s1.iloc[idx]) if pd.notna(s1.iloc[idx]) else None,
                    'target_value': str(s2.iloc[idx]) if pd.notna(s2.iloc[idx]) else None
                })
        
        return {
            'total_differences': len(differences),
            'differences': differences[:500],  # Limit total differences returned
            'comparison_method': 'positional'
        }
    
    def _compare_with_keys(self, df1: pd.DataFrame, df2: pd.DataFrame,
                          key_columns: List[str], compare_columns: List[str],
                          tolerance: float, ignore_case: bool, 
                          ignore_whitespace: bool) -> Dict:
        """Compare DataFrames using key columns for matching"""
        # Merge on keys
        merged = df1.merge(df2, on=key_columns, how='outer', 
                          suffixes=('_source', '_target'), indicator=True)
        
        only_in_source = merged[merged['_merge'] == 'left_only']
        only_in_target = merged[merged['_merge'] == 'right_only']
        in_both = merged[merged['_merge'] == 'both']
        
        # Compare values for matching rows
        value_differences = []
        for col in compare_columns:
            if col in key_columns:
                continue
            
            source_col = f"{col}_source"
            target_col = f"{col}_target"
            
            if source_col not in in_both.columns or target_col not in in_both.columns:
                continue
            
            s1 = in_both[source_col]
            s2 = in_both[target_col]
            
            if ignore_case and s1.dtype == object:
                s1 = s1.str.lower()
                s2 = s2.str.lower()
            if ignore_whitespace and s1.dtype == object:
                s1 = s1.str.strip()
                s2 = s2.str.strip()
            
            if np.issubdtype(s1.dtype, np.number) and tolerance > 0:
                diff_mask = ~(np.abs(s1 - s2) <= tolerance) & ~(s1.isna() & s2.isna())
            else:
                diff_mask = (s1 != s2) & ~(s1.isna() & s2.isna())
            
            diff_rows = in_both[diff_mask]
            
            for idx, row in diff_rows.head(100).iterrows():
                value_differences.append({
                    'keys': {k: str(row.get(k, row.get(f"{k}_source"))) for k in key_columns},
                    'column': col,
                    'source_value': str(row[source_col]) if pd.notna(row[source_col]) else None,
                    'target_value': str(row[target_col]) if pd.notna(row[target_col]) else None
                })
        
        return {
            'total_differences': len(value_differences) + len(only_in_source) + len(only_in_target),
            'only_in_source': len(only_in_source),
            'only_in_target': len(only_in_target),
            'matching_rows': len(in_both),
            'value_differences': value_differences[:500],
            'comparison_method': 'key_based'
        }
    
    def _calculate_statistics(self, df1: pd.DataFrame, df2: pd.DataFrame,
                             compare_columns: List[str]) -> Dict:
        """Calculate comparison statistics"""
        stats = {
            'source_rows': len(df1),
            'target_rows': len(df2),
            'source_columns': len(df1.columns),
            'target_columns': len(df2.columns),
            'column_stats': {}
        }
        
        for col in compare_columns:
            if col in df1.columns and col in df2.columns:
                col_stats = {}
                
                if np.issubdtype(df1[col].dtype, np.number):
                    col_stats['source'] = {
                        'min': float(df1[col].min()) if pd.notna(df1[col].min()) else None,
                        'max': float(df1[col].max()) if pd.notna(df1[col].max()) else None,
                        'mean': float(df1[col].mean()) if pd.notna(df1[col].mean()) else None,
                        'sum': float(df1[col].sum()) if pd.notna(df1[col].sum()) else None
                    }
                    col_stats['target'] = {
                        'min': float(df2[col].min()) if pd.notna(df2[col].min()) else None,
                        'max': float(df2[col].max()) if pd.notna(df2[col].max()) else None,
                        'mean': float(df2[col].mean()) if pd.notna(df2[col].mean()) else None,
                        'sum': float(df2[col].sum()) if pd.notna(df2[col].sum()) else None
                    }
                
                stats['column_stats'][col] = col_stats
        
        return stats
