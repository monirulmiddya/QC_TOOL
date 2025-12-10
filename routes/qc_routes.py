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
    """Compare two datasets"""
    try:
        data = request.get_json()
        source_session_id = data.get('source_session_id')
        target_session_id = data.get('target_session_id')
        key_columns = data.get('key_columns', [])
        compare_columns = data.get('compare_columns')
        tolerance = data.get('tolerance', 0)
        ignore_case = data.get('ignore_case', False)
        ignore_whitespace = data.get('ignore_whitespace', False)
        
        if not source_session_id or not target_session_id:
            return jsonify({'error': 'Both source_session_id and target_session_id are required'}), 400
        
        # Get DataFrames
        df1 = get_dataframe(source_session_id)
        df2 = get_dataframe(target_session_id)
        
        # Run comparison
        comparator = DatasetComparator()
        result = comparator.compare(
            df1, df2,
            key_columns=key_columns if key_columns else None,
            compare_columns=compare_columns,
            tolerance=tolerance,
            ignore_case=ignore_case,
            ignore_whitespace=ignore_whitespace
        )
        
        # Store for export
        import uuid
        result_id = str(uuid.uuid4())
        QC_RESULTS_STORE[result_id] = {
            'type': 'comparison',
            'source_session_id': source_session_id,
            'target_session_id': target_session_id,
            'result': result.to_dict()
        }
        
        return jsonify({
            'success': True,
            'result_id': result_id,
            **result.to_dict()
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
