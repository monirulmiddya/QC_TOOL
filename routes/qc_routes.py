"""
QC Routes
Endpoints for running QC checks and comparisons.
"""
import logging
from flask import Blueprint, request, jsonify
import pandas as pd

from qc_engine import get_available_rules, create_rule, DatasetComparator
from routes.data_routes import get_dataframe, DATA_STORE

bp = Blueprint('qc', __name__)
logger = logging.getLogger(__name__)

# Store QC results for export
QC_RESULTS_STORE = {}


def row_to_dict(row):
    """Convert a pandas row to dict while preserving date formats.
    
    Converts Timestamp objects to ISO format strings instead of the 
    verbose 'Mon, 01 Jan 2024 00:00:00 GMT' format.
    """
    from datetime import datetime, date
    result = {}
    for key, val in row.to_dict().items():
        if isinstance(val, pd.Timestamp):
            result[key] = val.isoformat() if pd.notna(val) else None
        elif isinstance(val, (datetime, date)):
            result[key] = val.isoformat()
        elif pd.isna(val):
            result[key] = None
        else:
            result[key] = val
    return result


@bp.route('/rules', methods=['GET'])
def list_rules():
    """List all available QC rules"""
    try:
        rules = get_available_rules()
        return jsonify({
            'success': True,
            'rules': rules
        })
    except Exception as e:
        logger.error(f"Failed to list rules: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/run', methods=['POST'])
def run_qc():
    """Run QC checks on loaded data"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        rules_config = data.get('rules', [])
        
        if not session_id:
            return jsonify({'error': 'session_id is required'}), 400
        
        if not rules_config:
            return jsonify({'error': 'At least one rule is required'}), 400
        
        # Get the DataFrame
        df = get_dataframe(session_id)
        
        # Run each rule
        results = []
        all_passed = True
        
        for rule_config in rules_config:
            rule_id = rule_config.get('rule_id')
            config = rule_config.get('config', {})
            
            if not rule_id:
                results.append({
                    'error': 'rule_id is required for each rule',
                    'passed': False
                })
                all_passed = False
                continue
            
            try:
                rule = create_rule(rule_id)
                result = rule.execute(df, config)
                results.append(result.to_dict())
                
                if not result.passed:
                    all_passed = False
                    
            except Exception as e:
                logger.error(f"Rule {rule_id} failed: {e}")
                results.append({
                    'rule_name': rule_id,
                    'passed': False,
                    'error': str(e)
                })
                all_passed = False
        
        # Store results for export
        import uuid
        result_id = str(uuid.uuid4())
        QC_RESULTS_STORE[result_id] = {
            'session_id': session_id,
            'results': results,
            'all_passed': all_passed
        }
        
        return jsonify({
            'success': True,
            'result_id': result_id,
            'all_passed': all_passed,
            'total_rules': len(results),
            'passed_count': sum(1 for r in results if r.get('passed', False)),
            'failed_count': sum(1 for r in results if not r.get('passed', True)),
            'results': results
        })
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"QC execution failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/compare', methods=['POST'])
def compare_datasets():
    """Compare multiple datasets - find duplicates, unique records, and differences"""
    try:
        data = request.get_json()
        session_ids = data.get('session_ids', [])
        key_columns = data.get('key_columns', [])
        value_columns = data.get('value_columns')
        join_type = data.get('join_type', 'full')
        column_mappings = data.get('column_mappings', [])
        
        # Parse tolerance options (support new and legacy format)
        tolerance_config = data.get('tolerance', {})
        if isinstance(tolerance_config, (int, float)):
            # Legacy format
            numeric_tolerance = tolerance_config
            tolerance_type = 'absolute'
            date_tolerance = 0
            date_tolerance_unit = 'days'
        else:
            numeric_tolerance = tolerance_config.get('numeric', 0)
            tolerance_type = tolerance_config.get('numeric_type', 'absolute')
            date_tolerance = tolerance_config.get('date', 0)
            date_tolerance_unit = tolerance_config.get('date_unit', 'days')
        
        # Parse options (support new and legacy format)
        options = data.get('options', {})
        ignore_case = options.get('ignore_case', data.get('ignore_case', False))
        ignore_whitespace = options.get('ignore_whitespace', data.get('ignore_whitespace', False))
        null_equals_null = options.get('null_equals_null', True)
        fuzzy_match = options.get('fuzzy_match', False)
        fuzzy_threshold = options.get('fuzzy_threshold', 80) / 100.0  # Convert to 0-1 ratio
        transformations = options.get('transformations', [])
        
        analysis = data.get('analysis', {})
        
        # Support legacy format (two datasets)
        if not session_ids:
            source_session_id = data.get('source_session_id')
            target_session_id = data.get('target_session_id')
            if source_session_id and target_session_id:
                session_ids = [source_session_id, target_session_id]
        
        if len(session_ids) < 2:
            return jsonify({'error': 'At least 2 session_ids are required'}), 400
        
        if not key_columns:
            return jsonify({'error': 'key_columns are required for matching'}), 400
        
        # Load all DataFrames
        dfs = {}
        source_order = []  # Track order for LEFT/RIGHT joins
        for sid in session_ids:
            stored = DATA_STORE.get(sid)
            if not stored:
                return jsonify({'error': f'Session not found: {sid}'}), 404
            source_name = stored.get('source_name', stored.get('source', sid[:8]))
            dfs[source_name] = get_dataframe(sid)
            source_order.append(source_name)
        
        # Determine columns to compare
        compare_cols = value_columns if value_columns else None
        
        # Analysis results
        result = {}
        
        # Apply transformations to a string value
        def apply_transformations(val, transforms):
            if val is None:
                return ''
            val = str(val)
            for t in transforms:
                if t == 'trim':
                    val = val.strip()
                elif t == 'lower':
                    val = val.lower()
                elif t == 'upper':
                    val = val.upper()
                elif t == 'remove_special':
                    import re
                    val = re.sub(r'[^a-zA-Z0-9\s]', '', val)
                elif t == 'normalize_spaces':
                    import re
                    val = re.sub(r'\s+', ' ', val).strip()
            return val
        
        # Simple fuzzy ratio (Levenshtein-based similarity)
        def fuzzy_ratio(s1, s2):
            if s1 == s2:
                return 1.0
            len1, len2 = len(s1), len(s2)
            if len1 == 0 or len2 == 0:
                return 0.0
            # Simple character match ratio
            matches = sum(1 for c1, c2 in zip(s1, s2) if c1 == c2)
            return matches / max(len1, len2)
        
        # Create combined key for each row
        def create_key(row, columns, ignore_case_flag, ignore_ws):
            vals = []
            for col in columns:
                val = row.get(col, '')
                if val is None:
                    val = ''
                val = str(val)
                # Apply transformations
                if transformations:
                    val = apply_transformations(val, transformations)
                if ignore_case_flag:
                    val = val.lower()
                if ignore_ws:
                    val = val.strip()
                vals.append(val)
            return tuple(vals)
        
        # Build key sets for each source
        key_sets = {}
        key_to_rows = {}
        
        for source_name, df in dfs.items():
            key_sets[source_name] = set()
            for idx, row in df.iterrows():
                key = create_key(row_to_dict(row), key_columns, ignore_case, ignore_whitespace)
                key_sets[source_name].add(key)
                
                if key not in key_to_rows:
                    key_to_rows[key] = {}
                if source_name not in key_to_rows[key]:
                    key_to_rows[key][source_name] = []
                key_to_rows[key][source_name].append(row_to_dict(row))
        
        # Find duplicates (keys appearing in 2+ sources)
        if analysis.get('duplicates', True):
            duplicate_keys = [k for k, sources in key_to_rows.items() if len(sources) > 1]
            duplicate_rows = []
            for key in duplicate_keys[:100]:  # Limit to 100
                for source, rows in key_to_rows[key].items():
                    for row in rows:
                        row_copy = dict(row)
                        row_copy['_source'] = source
                        duplicate_rows.append(row_copy)
            
            result['duplicates'] = {
                'count': len(duplicate_keys),
                'rows': duplicate_rows
            }
        
        # Find unique records (keys only in one source)
        if analysis.get('unique', True):
            result['unique'] = {}
            all_keys = set(key_to_rows.keys())
            
            for source_name in dfs.keys():
                unique_keys = [k for k in all_keys if len(key_to_rows[k]) == 1 and source_name in key_to_rows[k]]
                unique_rows = []
                for key in unique_keys[:100]:  # Limit
                    for row in key_to_rows[key][source_name]:
                        unique_rows.append(row)
                
                result['unique'][source_name] = {
                    'count': len(unique_keys),
                    'rows': unique_rows
                }
        
        # Find value differences (same key, different values)
        if analysis.get('not_matched', True):
            difference_rows = []
            column_differences = {}
            
            # Helper to compare values with tolerance and get status
            def compare_values(val1, val2, col):
                # Null handling
                is_null1 = pd.isna(val1) if not isinstance(val1, str) else val1 == ''
                is_null2 = pd.isna(val2) if not isinstance(val2, str) else val2 == ''
                
                if is_null1 and is_null2:
                    return ('MATCH', None) if null_equals_null else ('NULL_MISMATCH', 'Both null')
                if is_null1:
                    return ('NULL_MISMATCH', f'{val2} vs null')
                if is_null2:
                    return ('NULL_MISMATCH', f'{val1} vs null')
                
                # Numeric comparison with tolerance
                if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                    diff = abs(val1 - val2)
                    if tolerance_type == 'percent':
                        max_val = max(abs(val1), abs(val2))
                        if max_val > 0:
                            pct_diff = (diff / max_val) * 100
                            if pct_diff <= numeric_tolerance:
                                return ('MATCH', None)
                    else:  # absolute
                        if diff <= numeric_tolerance:
                            return ('MATCH', None)
                    return ('MISMATCH', f'{val1} vs {val2} (diff: {diff:.4f})')
                
                # Date comparison
                try:
                    from datetime import datetime, timedelta
                    date1 = pd.to_datetime(val1)
                    date2 = pd.to_datetime(val2)
                    if pd.notna(date1) and pd.notna(date2):
                        if date_tolerance_unit == 'hours':
                            max_diff = timedelta(hours=date_tolerance)
                        elif date_tolerance_unit == 'minutes':
                            max_diff = timedelta(minutes=date_tolerance)
                        else:
                            max_diff = timedelta(days=date_tolerance)
                        
                        if abs(date1 - date2) <= max_diff:
                            return ('MATCH', None)
                        return ('MISMATCH', f'{val1} vs {val2}')
                except:
                    pass
                
                # String comparison
                str1 = str(val1) if val1 is not None else ''
                str2 = str(val2) if val2 is not None else ''
                
                if ignore_case:
                    str1, str2 = str1.lower(), str2.lower()
                if ignore_whitespace:
                    str1, str2 = str1.strip(), str2.strip()
                    
                if str1 == str2:
                    return ('MATCH', None)
                return ('MISMATCH', f'{val1} vs {val2}')
            
            # Compare all matching keys
            for key, sources in key_to_rows.items():
                if len(sources) < 2:
                    continue
                    
                source_names = list(sources.keys())
                base_source = source_names[0]
                base_row = sources[base_source][0]
                
                for other_source in source_names[1:]:
                    other_row = sources[other_source][0]
                    
                    cols_to_check = compare_cols if compare_cols else [c for c in base_row.keys() if c not in key_columns]
                    
                    for col in cols_to_check:
                        if col in key_columns or col.startswith('_'):
                            continue
                            
                        val1 = base_row.get(col)
                        val2 = other_row.get(col)
                        
                        status, diff_detail = compare_values(val1, val2, col)
                        
                        if status != 'MATCH':
                            if col not in column_differences:
                                column_differences[col] = 0
                            column_differences[col] += 1
                            difference_rows.append({
                                'key': dict(zip(key_columns, key)),
                                'column': col,
                                'source1': base_source,
                                'value1': val1 if not pd.isna(val1) else None,
                                'source2': other_source,
                                'value2': val2 if not pd.isna(val2) else None,
                                'status': status,
                                'detail': diff_detail
                            })
            
            result['not_matched'] = {
                'count': len(difference_rows),
                'column_differences': column_differences,
                'rows': difference_rows[:100]
            }
        
        # Aggregation comparison
        aggregation_config = data.get('aggregation')
        if aggregation_config and aggregation_config.get('enabled'):
            agg_func = aggregation_config.get('function', 'sum')
            agg_column = aggregation_config.get('column')
            group_by_cols = aggregation_config.get('group_by', [])
            variance_threshold = aggregation_config.get('variance_threshold', 1.0)
            
            if agg_column:
                agg_results = []
                variances = []
                
                # Calculate aggregates for each source
                source_aggs = {}
                for source_name, df in dfs.items():
                    try:
                        if group_by_cols:
                            # Group by aggregation
                            if agg_func == 'sum':
                                grouped = df.groupby(group_by_cols)[agg_column].sum()
                            elif agg_func == 'count':
                                grouped = df.groupby(group_by_cols)[agg_column].count()
                            elif agg_func == 'avg':
                                grouped = df.groupby(group_by_cols)[agg_column].mean()
                            elif agg_func == 'min':
                                grouped = df.groupby(group_by_cols)[agg_column].min()
                            elif agg_func == 'max':
                                grouped = df.groupby(group_by_cols)[agg_column].max()
                            else:
                                grouped = df.groupby(group_by_cols)[agg_column].sum()
                            
                            source_aggs[source_name] = grouped.to_dict()
                        else:
                            # Total aggregation
                            if agg_func == 'sum':
                                val = df[agg_column].sum()
                            elif agg_func == 'count':
                                val = df[agg_column].count()
                            elif agg_func == 'avg':
                                val = df[agg_column].mean()
                            elif agg_func == 'min':
                                val = df[agg_column].min()
                            elif agg_func == 'max':
                                val = df[agg_column].max()
                            else:
                                val = df[agg_column].sum()
                            
                            source_aggs[source_name] = {'_total': float(val) if pd.notna(val) else 0}
                    except Exception as e:
                        logger.warning(f"Aggregation failed for {source_name}: {e}")
                        source_aggs[source_name] = {}
                
                # Compare aggregates across sources
                source_names = list(source_aggs.keys())
                if len(source_names) >= 2:
                    base_source = source_names[0]
                    base_aggs = source_aggs[base_source]
                    
                    for group_key in base_aggs.keys():
                        row = {'group': str(group_key) if group_key != '_total' else 'TOTAL'}
                        row[base_source] = base_aggs[group_key]
                        
                        for other_source in source_names[1:]:
                            other_val = source_aggs[other_source].get(group_key, 0)
                            row[other_source] = other_val
                            
                            # Calculate variance
                            base_val = base_aggs[group_key]
                            if base_val and base_val != 0:
                                variance_pct = abs((other_val - base_val) / base_val) * 100
                            else:
                                variance_pct = 100 if other_val else 0
                            
                            row['variance_pct'] = round(variance_pct, 2)
                            row['exceeds_threshold'] = variance_pct > variance_threshold
                            
                            if row['exceeds_threshold']:
                                variances.append(row.copy())
                        
                        agg_results.append(row)
                
                result['aggregation'] = {
                    'function': agg_func,
                    'column': agg_column,
                    'group_by': group_by_cols,
                    'total_groups': len(agg_results),
                    'results': agg_results[:100],
                    'variances': variances[:50],
                    'variance_threshold': variance_threshold
                }
        
        # Store for export
        import uuid
        result_id = str(uuid.uuid4())
        QC_RESULTS_STORE[result_id] = {
            'type': 'multi_comparison',
            'session_ids': session_ids,
            'result': result
        }
        
        return jsonify({
            'success': True,
            'result_id': result_id,
            **result
        })
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"Comparison failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/results/<result_id>', methods=['GET'])
def get_qc_result(result_id):
    """Get stored QC result"""
    if result_id not in QC_RESULTS_STORE:
        return jsonify({'error': 'Result not found'}), 404
    
    return jsonify({
        'success': True,
        **QC_RESULTS_STORE[result_id]
    })


# Accessor for export routes
def get_qc_results(result_id: str):
    """Get QC results by ID"""
    if result_id not in QC_RESULTS_STORE:
        raise ValueError(f"Result not found: {result_id}")
    return QC_RESULTS_STORE[result_id]
