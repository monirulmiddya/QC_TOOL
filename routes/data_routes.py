"""
Data Routes
Endpoints for data loading and querying.
"""
import os
import uuid
import logging
from flask import Blueprint, request, jsonify, current_app, session
import pandas as pd

from connectors import PostgresConnector, AthenaConnector, FileConnector

bp = Blueprint('data', __name__)
logger = logging.getLogger(__name__)

# In-memory storage for loaded data (in production, use Redis or similar)
DATA_STORE = {}


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
        
        # Store data with session ID
        session_id = str(uuid.uuid4())
        DATA_STORE[session_id] = {
            'data': df,
            'source': source,
            'source_name': f"{source.upper()} Query",
            'query': query,
            'columns': df.columns.tolist(),
            'row_count': len(df),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
        }
        
        logger.info(f"Query executed successfully. Session: {session_id}, Rows: {len(df)}")
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'columns': df.columns.tolist(),
            'row_count': len(df),
            'preview': df_to_records(df.head(100)),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
        })
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/upload', methods=['POST'])
def upload_files():
    """Upload Excel or CSV files - creates separate session for each file"""
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files provided'}), 400
        
        files = request.files.getlist('files')
        if not files:
            return jsonify({'error': 'No files selected'}), 400
        
        upload_folder = current_app.config['UPLOAD_FOLDER']
        os.makedirs(upload_folder, exist_ok=True)
        
        created_sessions = []
        
        for file in files:
            if file.filename == '':
                continue
            
            # Check extension
            ext = os.path.splitext(file.filename)[1].lower()
            if ext not in {'.csv', '.xlsx', '.xls'}:
                created_sessions.append({
                    'filename': file.filename,
                    'success': False,
                    'error': f'Unsupported file type: {ext}'
                })
                continue
            
            try:
                # Generate unique filename
                unique_name = f"{uuid.uuid4()}_{file.filename}"
                file_path = os.path.join(upload_folder, unique_name)
                file.save(file_path)
                
                # Load file using connector
                connector = FileConnector(file_path=file_path)
                df = connector.execute_query()
                
                # Create session for this file
                session_id = str(uuid.uuid4())
                DATA_STORE[session_id] = {
                    'data': df,
                    'source': 'file',
                    'source_name': file.filename,
                    'file_path': file_path,
                    'columns': df.columns.tolist(),
                    'row_count': len(df),
                    'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
                }
                
                created_sessions.append({
                    'session_id': session_id,
                    'filename': file.filename,
                    'success': True,
                    'columns': df.columns.tolist(),
                    'row_count': len(df),
                    'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
                })
                
                logger.info(f"File uploaded: {file.filename} -> Session: {session_id}, Rows: {len(df)}")
                
            except Exception as e:
                created_sessions.append({
                    'filename': file.filename,
                    'success': False,
                    'error': str(e)
                })
                logger.error(f"Failed to load {file.filename}: {e}")
        
        successful = [s for s in created_sessions if s.get('success')]
        failed = [s for s in created_sessions if not s.get('success')]
        
        return jsonify({
            'success': len(successful) > 0,
            'sessions': created_sessions,
            'total_files': len(created_sessions),
            'successful_count': len(successful),
            'failed_count': len(failed)
        })
        
    except Exception as e:
        logger.error(f"File upload failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/preview/<session_id>', methods=['GET'])
def get_preview(session_id):
    """Get preview of loaded data"""
    try:
        if session_id not in DATA_STORE:
            return jsonify({'error': 'Session not found'}), 404
        
        stored = DATA_STORE[session_id]
        df = stored['data']
        
        offset = request.args.get('offset', 0, type=int)
        limit = request.args.get('limit', 100, type=int)
        
        subset = df.iloc[offset:offset + limit]
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'columns': df.columns.tolist(),
            'total_rows': len(df),
            'offset': offset,
            'limit': limit,
            'data': df_to_records(subset),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
        })
        
    except Exception as e:
        logger.error(f"Preview failed: {e}")
        return jsonify({'error': str(e)}), 500


@bp.route('/sessions', methods=['GET'])
def list_sessions():
    """List all active data sessions with details"""
    sessions = []
    for sid, stored in DATA_STORE.items():
        session_info = {
            'session_id': sid,
            'source': stored['source'],
            'source_name': stored.get('source_name', stored['source']),
            'row_count': stored['row_count'],
            'column_count': len(stored['columns']),
            'columns': stored['columns'],
            'dtypes': stored.get('dtypes', {})
        }
        # Add query info for database sources
        if 'query' in stored:
            session_info['query'] = stored['query']
        sessions.append(session_info)
    
    return jsonify({'success': True, 'sessions': sessions, 'count': len(sessions)})


@bp.route('/sessions/<session_id>', methods=['DELETE'])
def delete_session(session_id):
    """Delete a data session"""
    if session_id not in DATA_STORE:
        return jsonify({'error': 'Session not found'}), 404
    
    # Clean up files if it was a file upload
    stored = DATA_STORE[session_id]
    if stored['source'] == 'file':
        # Handle new single file format
        if 'file_path' in stored:
            try:
                os.remove(stored['file_path'])
            except:
                pass
        # Handle old multi-file format (backwards compatibility)
        elif 'files' in stored:
            for file_info in stored['files']:
                try:
                    os.remove(file_info['path'])
                except:
                    pass
    
    del DATA_STORE[session_id]
    return jsonify({'success': True, 'message': 'Session deleted'})


@bp.route('/sessions/<session_id>/rename', methods=['PUT'])
def rename_session(session_id):
    """Rename a data session's source name"""
    if session_id not in DATA_STORE:
        return jsonify({'error': 'Session not found'}), 404
    
    data = request.get_json()
    new_name = data.get('name', '').strip()
    
    if not new_name:
        return jsonify({'error': 'Name is required'}), 400
    
    DATA_STORE[session_id]['source_name'] = new_name
    logger.info(f"Session {session_id} renamed to: {new_name}")
    return jsonify({'success': True, 'source_name': new_name})


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
def get_dataframe(session_id: str) -> pd.DataFrame:
    """Get DataFrame by session ID"""
    if session_id not in DATA_STORE:
        raise ValueError(f"Session not found: {session_id}")
    return DATA_STORE[session_id]['data']
