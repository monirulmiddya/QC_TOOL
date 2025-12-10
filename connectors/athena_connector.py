"""
AWS Athena Connector
Handles connections and queries to AWS Athena.
"""
import logging
import time
from typing import Optional, Dict, Any, Generator
import pandas as pd

try:
    import boto3
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

try:
    from pyathena import connect as athena_connect
    from pyathena.pandas.cursor import PandasCursor
    PYATHENA_AVAILABLE = True
except ImportError:
    PYATHENA_AVAILABLE = False

from .base import BaseConnector

logger = logging.getLogger(__name__)


class AthenaConnector(BaseConnector):
    """AWS Athena connector"""
    
    def __init__(self, region: str, s3_output: str, database: str,
                 access_key: str = None, secret_key: str = None,
                 workgroup: str = 'primary', **kwargs):
        super().__init__({
            'region': region,
            's3_output': s3_output,
            'database': database,
            'access_key': access_key,
            'secret_key': secret_key,
            'workgroup': workgroup,
            **kwargs
        })
        self._client = None
        self._pyathena_conn = None
    
    def connect(self) -> bool:
        """Establish connection to Athena"""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is not installed. Run: pip install boto3")
        
        try:
            # Create boto3 client
            session_kwargs = {'region_name': self.config['region']}
            if self.config.get('access_key') and self.config.get('secret_key'):
                session_kwargs['aws_access_key_id'] = self.config['access_key']
                session_kwargs['aws_secret_access_key'] = self.config['secret_key']
            
            session = boto3.Session(**session_kwargs)
            self._client = session.client('athena')
            
            # Also create pyathena connection for DataFrame support
            if PYATHENA_AVAILABLE:
                conn_kwargs = {
                    'region_name': self.config['region'],
                    's3_staging_dir': self.config['s3_output'],
                    'schema_name': self.config['database'],
                    'work_group': self.config['workgroup'],
                    'cursor_class': PandasCursor
                }
                if self.config.get('access_key') and self.config.get('secret_key'):
                    conn_kwargs['aws_access_key_id'] = self.config['access_key']
                    conn_kwargs['aws_secret_access_key'] = self.config['secret_key']
                
                self._pyathena_conn = athena_connect(**conn_kwargs)
            
            logger.info(f"Connected to Athena: {self.config['database']}")
            return True
            
        except Exception as e:
            logger.error(f"Athena connection failed: {e}")
            raise ConnectionError(f"Failed to connect to Athena: {e}")
    
    def disconnect(self) -> None:
        """Close Athena connection"""
        if self._pyathena_conn:
            self._pyathena_conn.close()
            self._pyathena_conn = None
        self._client = None
        logger.info("Athena connection closed")
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        if not self._client:
            self.connect()
        
        try:
            logger.info(f"Executing Athena query: {query[:100]}...")
            
            # Use pyathena for simpler DataFrame handling
            if self._pyathena_conn:
                cursor = self._pyathena_conn.cursor()
                cursor.execute(query)
                df = cursor.fetchall()
                if isinstance(df, pd.DataFrame):
                    logger.info(f"Query returned {len(df)} rows")
                    return df
                else:
                    # Convert to DataFrame if needed
                    df = pd.DataFrame(df)
                    logger.info(f"Query returned {len(df)} rows")
                    return df
            
            # Fallback to boto3 direct execution
            return self._execute_with_boto3(query)
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise RuntimeError(f"Query execution failed: {e}")
    
    def _execute_with_boto3(self, query: str) -> pd.DataFrame:
        """Execute query using boto3 directly"""
        # Start query execution
        response = self._client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.config['database']},
            ResultConfiguration={'OutputLocation': self.config['s3_output']},
            WorkGroup=self.config['workgroup']
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query to complete
        while True:
            status = self._client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = status['QueryExecution']['Status']['State']
            
            if state == 'SUCCEEDED':
                break
            elif state in ['FAILED', 'CANCELLED']:
                reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise RuntimeError(f"Query {state}: {reason}")
            
            time.sleep(1)
        
        # Get results
        results = self._client.get_query_results(QueryExecutionId=query_execution_id)
        
        # Parse results into DataFrame
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:  # Skip header row
            rows.append([field.get('VarCharValue', '') for field in row['Data']])
        
        return pd.DataFrame(rows, columns=columns)
    
    def get_schema(self) -> Dict[str, Any]:
        """Get database schema information"""
        if not self._client:
            self.connect()
        
        schema = {}
        
        # Get tables
        tables_query = f"SHOW TABLES IN {self.config['database']}"
        tables_df = self.execute_query(tables_query)
        
        for _, row in tables_df.iterrows():
            table_name = row.iloc[0]
            
            # Get columns for each table
            columns_query = f"DESCRIBE {self.config['database']}.{table_name}"
            try:
                columns_df = self.execute_query(columns_query)
                schema[table_name] = {
                    'columns': [
                        {'name': r.iloc[0], 'type': r.iloc[1] if len(r) > 1 else 'unknown'}
                        for _, r in columns_df.iterrows()
                    ]
                }
            except Exception as e:
                logger.warning(f"Could not get schema for {table_name}: {e}")
                schema[table_name] = {'columns': [], 'error': str(e)}
        
        return schema
    
    def get_databases(self) -> list:
        """Get list of available databases"""
        query = "SHOW DATABASES"
        df = self.execute_query(query)
        return df.iloc[:, 0].tolist()
    
    def get_tables(self) -> list:
        """Get list of tables in current database"""
        query = f"SHOW TABLES IN {self.config['database']}"
        df = self.execute_query(query)
        return df.iloc[:, 0].tolist()
