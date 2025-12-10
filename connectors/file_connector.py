"""
File Connector
Handles file uploads and parsing for Excel and CSV files.
"""
import os
import logging
from typing import Optional, Dict, Any, List, Generator
import pandas as pd

from .base import BaseConnector

logger = logging.getLogger(__name__)


class FileConnector(BaseConnector):
    """File-based data connector for CSV and Excel files"""
    
    SUPPORTED_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
    
    def __init__(self, file_path: str = None, file_paths: List[str] = None, **kwargs):
        super().__init__(kwargs)
        self.file_paths = file_paths or ([file_path] if file_path else [])
        self._dataframes = {}
    
    def connect(self) -> bool:
        """Validate file paths exist"""
        for path in self.file_paths:
            if not os.path.exists(path):
                raise FileNotFoundError(f"File not found: {path}")
            
            ext = os.path.splitext(path)[1].lower()
            if ext not in self.SUPPORTED_EXTENSIONS:
                raise ValueError(f"Unsupported file type: {ext}")
        
        logger.info(f"Validated {len(self.file_paths)} file(s)")
        return True
    
    def disconnect(self) -> None:
        """Clear loaded dataframes"""
        self._dataframes = {}
        logger.info("File connector cleared")
    
    def add_file(self, file_path: str) -> None:
        """Add a file to the connector"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {ext}")
        
        self.file_paths.append(file_path)
        logger.info(f"Added file: {file_path}")
    
    def execute_query(self, query: str = None, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Load and return data from files.
        For file connector, 'query' can be the file path or index.
        If no query specified, returns combined data from all files.
        """
        if not self.file_paths:
            raise ValueError("No files loaded")
        
        # If query is a number, use it as file index
        if query and query.isdigit():
            idx = int(query)
            if idx < len(self.file_paths):
                return self._load_file(self.file_paths[idx])
        
        # If query is a file path, load that specific file
        if query and query in self.file_paths:
            return self._load_file(query)
        
        # Default: return combined data from all files
        return self._load_all_files()
    
    def _load_file(self, file_path: str, chunk_size: int = None) -> pd.DataFrame:
        """Load a single file into DataFrame"""
        if file_path in self._dataframes:
            return self._dataframes[file_path]
        
        ext = os.path.splitext(file_path)[1].lower()
        logger.info(f"Loading file: {file_path}")
        
        try:
            if ext == '.csv':
                if chunk_size:
                    # Return iterator for chunked loading
                    return pd.read_csv(file_path, chunksize=chunk_size)
                df = pd.read_csv(file_path)
            elif ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, engine='openpyxl')
            else:
                raise ValueError(f"Unsupported file type: {ext}")
            
            self._dataframes[file_path] = df
            logger.info(f"Loaded {len(df)} rows from {os.path.basename(file_path)}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            raise RuntimeError(f"Failed to load file: {e}")
    
    def _load_all_files(self) -> pd.DataFrame:
        """Load and concatenate all files"""
        if not self.file_paths:
            return pd.DataFrame()
        
        dfs = []
        for path in self.file_paths:
            df = self._load_file(path)
            df['_source_file'] = os.path.basename(path)
            dfs.append(df)
        
        if len(dfs) == 1:
            return dfs[0]
        
        return pd.concat(dfs, ignore_index=True)
    
    def execute_chunked(self, query: str = None, chunk_size: int = 10000,
                       params: Optional[Dict] = None) -> Generator[pd.DataFrame, None, None]:
        """Load file in chunks for memory-efficient processing"""
        if not self.file_paths:
            raise ValueError("No files loaded")
        
        file_path = query if query in self.file_paths else self.file_paths[0]
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext == '.csv':
            # CSV supports native chunking
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                yield chunk
        else:
            # Excel files need to be loaded fully then chunked
            df = self._load_file(file_path)
            for start in range(0, len(df), chunk_size):
                yield df.iloc[start:start + chunk_size]
    
    def get_schema(self) -> Dict[str, Any]:
        """Get schema information for loaded files"""
        schema = {}
        
        for path in self.file_paths:
            try:
                # Load first few rows to get schema
                ext = os.path.splitext(path)[1].lower()
                if ext == '.csv':
                    df = pd.read_csv(path, nrows=5)
                else:
                    df = pd.read_excel(path, nrows=5, engine='openpyxl')
                
                schema[os.path.basename(path)] = {
                    'columns': [
                        {'name': col, 'type': str(df[col].dtype)}
                        for col in df.columns
                    ],
                    'row_count': self._get_row_count(path)
                }
            except Exception as e:
                schema[os.path.basename(path)] = {'error': str(e)}
        
        return schema
    
    def _get_row_count(self, file_path: str) -> int:
        """Get row count for a file"""
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext == '.csv':
            # Count lines efficiently without loading full file
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return sum(1 for _ in f) - 1  # Subtract header
        else:
            # For Excel, we need to load it
            if file_path in self._dataframes:
                return len(self._dataframes[file_path])
            df = pd.read_excel(file_path, engine='openpyxl')
            return len(df)
    
    def get_file_info(self) -> List[Dict[str, Any]]:
        """Get information about loaded files"""
        info = []
        for path in self.file_paths:
            stat = os.stat(path)
            info.append({
                'path': path,
                'name': os.path.basename(path),
                'size': stat.st_size,
                'extension': os.path.splitext(path)[1].lower()
            })
        return info
    
    @staticmethod
    def from_upload(file_storage, upload_folder: str) -> 'FileConnector':
        """Create connector from Flask file upload"""
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)
        
        filename = file_storage.filename
        file_path = os.path.join(upload_folder, filename)
        file_storage.save(file_path)
        
        logger.info(f"Saved uploaded file: {file_path}")
        return FileConnector(file_path=file_path)
