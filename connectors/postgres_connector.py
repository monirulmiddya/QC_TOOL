"""
PostgreSQL Connector
Handles connections and queries to PostgreSQL databases.
"""
import logging
from typing import Optional, Dict, Any, Generator
import pandas as pd

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

from .base import BaseConnector

logger = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):
    """PostgreSQL database connector"""
    
    def __init__(self, host: str, port: int, database: str, 
                 user: str, password: str, **kwargs):
        super().__init__({
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password,
            **kwargs
        })
        self._connection = None
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL"""
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 is not installed. Run: pip install psycopg2-binary")
        
        try:
            self._connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                connect_timeout=30
            )
            logger.info(f"Connected to PostgreSQL: {self.config['database']}")
            return True
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("PostgreSQL connection closed")
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        if not self._connection:
            self.connect()
        
        try:
            logger.info(f"Executing PostgreSQL query: {query[:100]}...")
            
            # Use pandas read_sql for efficient DataFrame creation
            df = pd.read_sql(query, self._connection, params=params)
            
            logger.info(f"Query returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise RuntimeError(f"Query execution failed: {e}")
    
    def execute_chunked(self, query: str, chunk_size: int = 10000,
                       params: Optional[Dict] = None) -> Generator[pd.DataFrame, None, None]:
        """Execute query and yield results in chunks"""
        if not self._connection:
            self.connect()
        
        try:
            # Use pandas read_sql with chunksize for memory-efficient loading
            for chunk in pd.read_sql(query, self._connection, params=params, 
                                    chunksize=chunk_size):
                yield chunk
                
        except Exception as e:
            logger.error(f"Chunked query execution failed: {e}")
            raise RuntimeError(f"Chunked query execution failed: {e}")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get database schema information"""
        if not self._connection:
            self.connect()
        
        schema_query = """
        SELECT 
            table_schema,
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns 
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name, ordinal_position
        """
        
        df = self.execute_query(schema_query)
        
        schema = {}
        for _, row in df.iterrows():
            table_key = f"{row['table_schema']}.{row['table_name']}"
            if table_key not in schema:
                schema[table_key] = {'columns': []}
            schema[table_key]['columns'].append({
                'name': row['column_name'],
                'type': row['data_type'],
                'nullable': row['is_nullable'] == 'YES'
            })
        
        return schema
    
    def get_tables(self) -> list:
        """Get list of available tables"""
        query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
        """
        df = self.execute_query(query)
        return [f"{row['table_schema']}.{row['table_name']}" for _, row in df.iterrows()]
