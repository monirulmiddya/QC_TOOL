"""
SQLite3 Storage Module
Handles persistent storage for credentials, templates, and settings.
"""
import sqlite3
import json
import os
from typing import Optional, Dict, List, Any
from contextlib import contextmanager

# Database file in root directory
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'qc_tool.db')


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Enable foreign keys for CASCADE DELETE to work
    conn.execute('PRAGMA foreign_keys = ON')
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_database():
    """Initialize database tables"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Credentials table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS credentials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(type, name)
            )
        ''')
        

        
        # Settings table (for future use)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Data Sources table - stores metadata about loaded data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_sources (
                source_id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                columns TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                query TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Source Data table - stores the actual data rows
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS source_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
                row_data TEXT NOT NULL,
                FOREIGN KEY (source_id) REFERENCES data_sources(source_id) ON DELETE CASCADE
            )
        ''')
        
        # Create index for faster data retrieval
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_source_data_source_id 
            ON source_data(source_id)
        ''')
        
        print(f"Database initialized at: {DB_PATH}")


# ============ Credentials Functions ============

def save_credential(cred_type: str, name: str, data: Dict) -> bool:
    """Save or update a credential"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO credentials (type, name, data, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(type, name) DO UPDATE SET
                data = excluded.data,
                updated_at = CURRENT_TIMESTAMP
        ''', (cred_type, name, json.dumps(data)))
        return True


def get_credential(cred_type: str, name: str) -> Optional[Dict]:
    """Get a specific credential"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT data FROM credentials WHERE type = ? AND name = ?',
            (cred_type, name)
        )
        row = cursor.fetchone()
        if row:
            return json.loads(row['data'])
        return None


def list_credentials(cred_type: str) -> List[str]:
    """List all credential names for a type"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT name FROM credentials WHERE type = ? ORDER BY name',
            (cred_type,)
        )
        return [row['name'] for row in cursor.fetchall()]


def delete_credential(cred_type: str, name: str) -> bool:
    """Delete a credential"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'DELETE FROM credentials WHERE type = ? AND name = ?',
            (cred_type, name)
        )
        return cursor.rowcount > 0





# ============ Settings Functions ============

def set_setting(key: str, value: Any) -> bool:
    """Save a setting"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO settings (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
        ''', (key, json.dumps(value)))
        return True


def get_setting(key: str, default: Any = None) -> Any:
    """Get a setting"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        row = cursor.fetchone()
        if row:
            return json.loads(row['value'])
        return default


# ============ Data Sources Functions ============

def save_data_source(source_id: str, source_name: str, source_type: str, 
                     columns: List[str], data: List[Dict], query: str = None) -> bool:
    """Save a data source with its data rows"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Delete existing data if session exists
        cursor.execute('DELETE FROM source_data WHERE source_id = ?', (source_id,))
        cursor.execute('DELETE FROM data_sources WHERE source_id = ?', (source_id,))
        
        # Insert source metadata
        cursor.execute('''
            INSERT INTO data_sources (source_id, source_name, source_type, columns, row_count, query)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (source_id, source_name, source_type, json.dumps(columns), len(data), query))
        
        # Insert data rows in batches for performance
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(
                'INSERT INTO source_data (source_id, row_data) VALUES (?, ?)',
                [(source_id, json.dumps(row)) for row in batch]
            )
        
        return True


def get_data_source(source_id: str) -> Optional[Dict]:
    """Get a data source with its data as a list of dicts"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get metadata
        cursor.execute(
            'SELECT * FROM data_sources WHERE source_id = ?',
            (source_id,)
        )
        meta = cursor.fetchone()
        if not meta:
            return None
        
        # Get data rows
        cursor.execute(
            'SELECT row_data FROM source_data WHERE source_id = ? ORDER BY id',
            (source_id,)
        )
        rows = [json.loads(row['row_data']) for row in cursor.fetchall()]
        
        return {
            'source_id': meta['source_id'],
            'source_name': meta['source_name'],
            'source': meta['source_type'],
            'columns': json.loads(meta['columns']),
            'row_count': meta['row_count'],
            'query': meta['query'],
            'data': rows
        }


def list_data_sources() -> List[Dict]:
    """List all data sources (metadata only, no data)"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM data_sources ORDER BY created_at DESC')
        sources = []
        for row in cursor.fetchall():
            sources.append({
                'source_id': row['source_id'],
                'source_name': row['source_name'],
                'source': row['source_type'],
                'columns': json.loads(row['columns']),
                'row_count': row['row_count'],
                'column_count': len(json.loads(row['columns'])),
                'query': row['query'],
                'created_at': row['created_at']
            })
        return sources


def delete_data_source(source_id: str) -> bool:
    """Delete a data source and its data"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM source_data WHERE source_id = ?', (source_id,))
        cursor.execute('DELETE FROM data_sources WHERE source_id = ?', (source_id,))
        return cursor.rowcount > 0


def update_source_name(source_id: str, new_name: str) -> bool:
    """Update the name of a data source"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE data_sources SET source_name = ? WHERE source_id = ?',
            (new_name, source_id)
        )
        return cursor.rowcount > 0


def clear_all_sources() -> int:
    """Delete all data sources (for cleanup)"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM source_data')
        cursor.execute('DELETE FROM data_sources')
        return cursor.rowcount


def get_unique_source_name(base_name: str) -> str:
    """Get a unique source name by appending (1), (2), etc. if needed"""
    sources = list_data_sources()
    existing_names = {s['source_name'] for s in sources}
    
    if base_name not in existing_names:
        return base_name
    
    counter = 1
    while f"{base_name} ({counter})" in existing_names:
        counter += 1
    
    return f"{base_name} ({counter})"


# Initialize database on module import
init_database()
