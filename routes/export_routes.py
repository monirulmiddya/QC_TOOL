"""
Export Routes
Endpoints for exporting QC results.
"""
import io
import logging
from flask import Blueprint, request, jsonify, send_file
import pandas as pd

from routes.qc_routes import get_qc_results
from routes.data_routes import get_dataframe

bp = Blueprint('export', __name__)
logger = logging.getLogger(__name__)


@bp.route('/csv', methods=['POST'])
def export_csv():
    """Export QC results as CSV"""
    try:
        data = request.get_json()
        result_id = data.get('result_id')
        include_failed_rows = data.get('include_failed_rows', True)
        
        if not result_id:
            return jsonify({'error': 'result_id is required'}), 400
        
        qc_results = get_qc_results(result_id)
        
        # Build export data
        export_data = _build_export_data(qc_results, include_failed_rows)
        
        # Create CSV
        output = io.StringIO()
        
        # Write summary
        output.write("QC Results Summary\n")
        output.write("=" * 50 + "\n\n")
        
        summary_df = pd.DataFrame(export_data['summary'])
        summary_df.to_csv(output, index=False)
        
        # Write detailed results
        if include_failed_rows and export_data.get('failed_rows'):
            output.write("\n\nFailed Rows Detail\n")
            output.write("=" * 50 + "\n\n")
            
            for rule_name, rows in export_data['failed_rows'].items():
                output.write(f"\n{rule_name}\n")
                output.write("-" * 30 + "\n")
                if rows:
                    rows_df = pd.DataFrame(rows)
                    rows_df.to_csv(output, index=False)
        
        output.seek(0)
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'qc_results_{result_id[:8]}.csv'
        )
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"CSV export failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/excel', methods=['POST'])
def export_excel():
    """Export QC results as Excel"""
    try:
        data = request.get_json()
        result_id = data.get('result_id')
        include_failed_rows = data.get('include_failed_rows', True)
        
        if not result_id:
            return jsonify({'error': 'result_id is required'}), 400
        
        qc_results = get_qc_results(result_id)
        
        # Build export data
        export_data = _build_export_data(qc_results, include_failed_rows)
        
        # Create Excel file
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4F81BD',
                'font_color': 'white',
                'border': 1
            })
            pass_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
            fail_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            
            # Summary sheet
            summary_df = pd.DataFrame(export_data['summary'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False, startrow=1)
            
            worksheet = writer.sheets['Summary']
            worksheet.write(0, 0, 'QC Results Summary', workbook.add_format({'bold': True, 'font_size': 14}))
            
            # Format headers
            for col_num, value in enumerate(summary_df.columns.values):
                worksheet.write(1, col_num, value, header_format)
            
            # Conditional formatting for pass/fail
            for row_num, passed in enumerate(summary_df['Passed'] if 'Passed' in summary_df.columns else []):
                if passed:
                    worksheet.write(row_num + 2, summary_df.columns.get_loc('Passed'), 'PASS', pass_format)
                else:
                    worksheet.write(row_num + 2, summary_df.columns.get_loc('Passed'), 'FAIL', fail_format)
            
            # Failed rows sheets
            if include_failed_rows and export_data.get('failed_rows'):
                for rule_name, rows in export_data['failed_rows'].items():
                    if rows:
                        # Clean sheet name (Excel has 31 char limit)
                        sheet_name = rule_name[:31].replace('/', '-').replace(':', '-')
                        rows_df = pd.DataFrame(rows)
                        rows_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                        
                        worksheet = writer.sheets[sheet_name]
                        worksheet.write(0, 0, f'Failed Rows: {rule_name}', 
                                       workbook.add_format({'bold': True, 'font_size': 12}))
            
            # Comparison results if present
            if 'comparison' in export_data:
                comp_df = pd.DataFrame(export_data['comparison'])
                comp_df.to_excel(writer, sheet_name='Comparison', index=False, startrow=1)
                
                worksheet = writer.sheets['Comparison']
                worksheet.write(0, 0, 'Dataset Comparison', 
                               workbook.add_format({'bold': True, 'font_size': 14}))
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'qc_results_{result_id[:8]}.xlsx'
        )
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"Excel export failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/json', methods=['POST'])
def export_json():
    """Export QC results as JSON"""
    try:
        data = request.get_json()
        result_id = data.get('result_id')
        include_failed_rows = data.get('include_failed_rows', True)
        
        if not result_id:
            return jsonify({'error': 'result_id is required'}), 400
        
        qc_results = get_qc_results(result_id)
        
        # Build export data
        export_data = _build_export_data(qc_results, include_failed_rows)
        
        # Create JSON file
        import json
        json_content = json.dumps(export_data, indent=2, default=str)
        
        return send_file(
            io.BytesIO(json_content.encode('utf-8')),
            mimetype='application/json',
            as_attachment=True,
            download_name=f'qc_results_{result_id[:8]}.json'
        )
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"JSON export failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/data/csv', methods=['POST'])
def export_data_csv():
    """Export loaded data as CSV"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({'error': 'session_id is required'}), 400
        
        df = get_dataframe(session_id)
        
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'data_{session_id[:8]}.csv'
        )
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"Data export failed: {e}")
        return jsonify({'error': str(e)}), 500


def _build_export_data(qc_results: dict, include_failed_rows: bool) -> dict:
    """Build export data structure from QC results"""
    export_data = {
        'summary': [],
        'failed_rows': {}
    }
    
    # Handle multi-source comparison results (from /api/qc/compare)
    if qc_results.get('type') == 'multi_comparison':
        result = qc_results['result']
        
        # Duplicates section
        if 'duplicates' in result:
            dup = result['duplicates']
            export_data['summary'].append({
                'Type': 'Duplicates (In Multiple Sources)',
                'Count': dup.get('count', 0),
                'Status': 'PASS' if dup.get('count', 0) == 0 else 'FAIL'
            })
            if include_failed_rows and dup.get('rows'):
                export_data['failed_rows']['Duplicates'] = dup['rows']
        
        # Unique records section
        if 'unique' in result:
            for source_name, info in result['unique'].items():
                export_data['summary'].append({
                    'Type': f'Unique to: {source_name}',
                    'Count': info.get('count', 0),
                    'Status': 'INFO'
                })
                if include_failed_rows and info.get('rows'):
                    export_data['failed_rows'][f'Unique_{source_name}'] = info['rows']
        
        # Not matched / Value differences section
        if 'not_matched' in result:
            diff = result['not_matched']
            export_data['summary'].append({
                'Type': 'Value Differences',
                'Count': diff.get('count', 0),
                'Status': 'PASS' if diff.get('count', 0) == 0 else 'FAIL'
            })
            if include_failed_rows and diff.get('rows'):
                export_data['failed_rows']['Differences'] = diff['rows']
            
            # Add column difference breakdown
            if diff.get('column_differences'):
                for col, count in diff['column_differences'].items():
                    export_data['summary'].append({
                        'Type': f'Column Difference: {col}',
                        'Count': count,
                        'Status': 'DETAIL'
                    })
        
        # Aggregation section
        if 'aggregation' in result:
            agg = result['aggregation']
            export_data['summary'].append({
                'Type': f'Aggregation: {agg.get("function", "").upper()}({agg.get("column", "")})',
                'Count': agg.get('total_groups', 0),
                'Status': 'PASS' if not agg.get('variances') else 'FAIL'
            })
            if agg.get('results'):
                export_data['aggregation'] = agg['results']
            if include_failed_rows and agg.get('variances'):
                export_data['failed_rows']['Aggregation_Variances'] = agg['variances']
        
        return export_data
    
    # Handle legacy comparison results (backward compatibility)
    if qc_results.get('type') == 'comparison':
        result = qc_results['result']
        export_data['summary'].append({
            'Type': 'Dataset Comparison',
            'Match': result['match'],
            'Message': result['message'],
            'Rows Compared': result['summary'].get('rows_compared', 0),
            'Total Differences': result['summary'].get('total_differences', 0)
        })
        
        if 'row_differences' in result and result['row_differences'].get('differences'):
            export_data['comparison'] = result['row_differences']['differences']
        
        return export_data
    
    # Handle QC rule results
    results = qc_results.get('results', [])
    
    for result in results:
        summary_row = {
            'Rule': result.get('rule_name', 'Unknown'),
            'Passed': result.get('passed', False),
            'Message': result.get('message', '')
        }
        
        # Add statistics if present
        stats = result.get('statistics', {})
        for key, value in stats.items():
            if isinstance(value, (int, float, str, bool)):
                summary_row[key.replace('_', ' ').title()] = value
        
        export_data['summary'].append(summary_row)
        
        # Add failed rows if present
        if include_failed_rows and result.get('failed_rows'):
            rule_name = result.get('rule_name', 'Unknown')
            export_data['failed_rows'][rule_name] = result['failed_rows']
    
    return export_data

