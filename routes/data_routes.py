"""
Data Routes
Endpoints for data loading and querying.
"""
import os
import uuid
import logging
from flask import Blueprint, request, jsonify, current_app
import pandas as pd

from connectors import PostgresConnector, AthenaConnector, FileConnector
import storage

bp = Blueprint('data', __name__)
logger = logging.getLogger(__name__)


def df_to_records(df):
    """Convert DataFrame to records while preserving original date formats.
    
    Pandas to_dict('records') converts datetime to verbose format like 
    'Mon, 01 Jan 2024 00:00:00 GMT'. This function keeps dates as ISO strings
    like '2024-01-01' or '2024-01-01T12:30:00' (time only if non-midnight).
    """
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Convert datetime columns to string format
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            # Convert to ISO format string, keeping time only if not midnight
            def format_datetime(x):
                if pd.isna(x):
                    return None
                # If time is midnight, just return date part
                if x.hour == 0 and x.minute == 0 and x.second == 0 and x.microsecond == 0:
                    return x.strftime('%Y-%m-%d')
                return x.isoformat()
            
            df_copy[col] = df_copy[col].apply(format_datetime)
    
    return df_copy.to_dict('records')


@bp.route('/query', methods=['POST'])
def execute_query():
    """Execute SQL query on Athena or PostgreSQL"""
    try:
        data = request.get_json()
        source = data.get('source', '').lower()
        query = data.get('query', '')
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        if source == 'postgres':
            connector = PostgresConnector(
                host=data.get('host') or current_app.config['POSTGRES_HOST'],
                port=int(data.get('port') or current_app.config['POSTGRES_PORT']),
                database=data.get('database') or current_app.config['POSTGRES_DB'],
                user=data.get('user') or current_app.config['POSTGRES_USER'],
                password=data.get('password') or current_app.config['POSTGRES_PASSWORD']
            )
        elif source == 'athena':
            connector = AthenaConnector(
                region=data.get('region') or current_app.config['AWS_REGION'],
                s3_output=data.get('s3_output') or current_app.config['ATHENA_S3_OUTPUT'],
                database=data.get('database') or current_app.config['ATHENA_DATABASE'],
                access_key=data.get('access_key') or current_app.config['AWS_ACCESS_KEY_ID'],
                secret_key=data.get('secret_key') or current_app.config['AWS_SECRET_ACCESS_KEY'],
                workgroup=data.get('workgroup') or current_app.config['ATHENA_WORKGROUP']
            )
        else:
            return jsonify({'error': f'Invalid source: {source}. Use "postgres" or "athena"'}), 400
        
        with connector:
            df = connector.execute_query(query)
        
        # Store data in SQLite
        source_id = str(uuid.uuid4())
        unique_name = storage.get_unique_source_name(f"{source.upper()} Query")
        columns = df.columns.tolist()
        data_records = df_to_records(df)
        
        storage.save_data_source(
            source_id=source_id,
            source_name=unique_name,
            source_type=source,
            columns=columns,
            data=data_records,
            query=query
        )
        
        logger.info(f"Query executed successfully. Source: {source_id}, Rows: {len(df)}")
        
        return jsonify({
            'success': True,
            'source_id': source_id,
            'columns': columns,
            'row_count': len(df),
            'preview': data_records[:100],
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
        })
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/upload', methods=['POST'])
def upload_files():
    """Upload Excel or CSV files - creates separate source for each file"""
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files provided'}), 400
        
        files = request.files.getlist('files')
        if not files:
            return jsonify({'error': 'No files selected'}), 400
        
        upload_folder = current_app.config['UPLOAD_FOLDER']
        os.makedirs(upload_folder, exist_ok=True)
        
        created_sources = []
        
        for file in files:
            if file.filename == '':
                continue
            
            # Check extension
            ext = os.path.splitext(file.filename)[1].lower()
            if ext not in {'.csv', '.xlsx', '.xls'}:
                created_sources.append({
                    'filename': file.filename,
                    'success': False,
                    'error': f'Unsupported file type: {ext}'
                })
                continue
            
            try:
                # Generate unique filename
                unique_filename = f"{uuid.uuid4()}_{file.filename}"
                file_path = os.path.join(upload_folder, unique_filename)
                file.save(file_path)
                
                # Load file using connector
                connector = FileConnector(file_path=file_path)
                df = connector.execute_query()
                
                # Create source for this file
                source_id = str(uuid.uuid4())
                unique_name = storage.get_unique_source_name(file.filename)
                columns = df.columns.tolist()
                data_records = df_to_records(df)
                
                # Save to SQLite
                storage.save_data_source(
                    source_id=source_id,
                    source_name=unique_name,
                    source_type='file',
                    columns=columns,
                    data=data_records,
                    query=file_path  # Store file path in query field for reference
                )
                
                created_sources.append({
                    'source_id': source_id,
                    'filename': file.filename,
                    'success': True,
                    'columns': columns,
                    'row_count': len(df),
                    'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
                })
                
                logger.info(f"File uploaded: {file.filename} -> Source: {source_id}, Rows: {len(df)}")
                
            except Exception as e:
                created_sources.append({
                    'filename': file.filename,
                    'success': False,
                    'error': str(e)
                })
                logger.error(f"Failed to load {file.filename}: {e}")
        
        successful = [s for s in created_sources if s.get('success')]
        failed = [s for s in created_sources if not s.get('success')]
        
        return jsonify({
            'success': len(successful) > 0,
            'sources': created_sources,
            'total_files': len(created_sources),
            'successful_count': len(successful),
            'failed_count': len(failed)
        })
        
    except Exception as e:
        logger.error(f"File upload failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/preview/<source_id>', methods=['GET'])
def get_preview(source_id):
    """Get preview of loaded data"""
    try:
        source_data = storage.get_data_source(source_id)
        if not source_data:
            return jsonify({'error': 'Source not found'}), 404
        
        offset = request.args.get('offset', 0, type=int)
        limit = request.args.get('limit', 100, type=int)
        
        data = source_data['data']
        subset = data[offset:offset + limit]
        
        return jsonify({
            'success': True,
            'source_id': source_id,
            'columns': source_data['columns'],
            'total_rows': source_data['row_count'],
            'offset': offset,
            'limit': limit,
            'data': subset
        })
        
    except Exception as e:
        logger.error(f"Preview failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/sources', methods=['GET'])
def list_sources():
    """List all data sources with details"""
    try:
        sources = storage.list_data_sources()
        return jsonify({'success': True, 'sources': sources, 'count': len(sources)})
    except Exception as e:
        logger.error(f"List sources failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/sources/<source_id>', methods=['DELETE'])
def delete_source(source_id):
    """Delete a data source and all related data"""
    try:
        source_data = storage.get_data_source(source_id)
        if not source_data:
            return jsonify({'error': 'Source not found'}), 404
        
        # Clean up uploaded files
        if source_data['source'] == 'file' and source_data.get('query'):
            try:
                file_path = source_data['query']
                if os.path.exists(file_path):
                    os.remove(file_path)
            except:
                pass
        
        storage.delete_data_source(source_id)
        return jsonify({'success': True, 'message': 'Source deleted'})
    except Exception as e:
        logger.error(f"Delete source failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/sources/<source_id>/rename', methods=['PUT'])
def rename_source(source_id):
    """Rename a data source"""
    try:
        source_data = storage.get_data_source(source_id)
        if not source_data:
            return jsonify({'error': 'Source not found'}), 404
        
        data = request.get_json()
        new_name = data.get('name', '').strip()
        
        if not new_name:
            return jsonify({'error': 'Name is required'}), 400
        
        storage.update_source_name(source_id, new_name)
        logger.info(f"Source {source_id} renamed to: {new_name}")
        return jsonify({'success': True, 'source_name': new_name})
    except Exception as e:
        logger.error(f"Rename source failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/test-connection', methods=['POST'])
def test_connection():
    """Test database connection"""
    try:
        data = request.get_json()
        source = data.get('source', '').lower()
        
        if source == 'postgres':
            connector = PostgresConnector(
                host=data.get('host', ''),
                port=int(data.get('port', 5432)),
                database=data.get('database', ''),
                user=data.get('user', ''),
                password=data.get('password', '')
            )
        elif source == 'athena':
            connector = AthenaConnector(
                region=data.get('region', ''),
                s3_output=data.get('s3_output', ''),
                database=data.get('database', ''),
                access_key=data.get('access_key', ''),
                secret_key=data.get('secret_key', ''),
                workgroup=data.get('workgroup', 'primary')
            )
        else:
            return jsonify({'error': f'Invalid source: {source}'}), 400
        
        result = connector.test_connection()
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


# Accessor function for other modules
def get_dataframe(source_id: str) -> pd.DataFrame:
    """Get DataFrame by source ID"""
    source_data = storage.get_data_source(source_id)
    if not source_data:
        raise ValueError(f"Source not found: {source_id}")
    
    # Convert list of dicts back to DataFrame
    return pd.DataFrame(source_data['data'])
