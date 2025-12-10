"""
Base Connector Abstract Class
Defines the interface for all data source connectors.
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Generator
import pandas as pd


class BaseConnector(ABC):
    """Abstract base class for all data connectors"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self._connection = None
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame"""
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema/metadata of the data source"""
        pass
    
    def execute_chunked(self, query: str, chunk_size: int = 10000, 
                       params: Optional[Dict] = None) -> Generator[pd.DataFrame, None, None]:
        """
        Execute query and yield results in chunks for large datasets.
        Default implementation - subclasses can override for optimization.
        """
        df = self.execute_query(query, params)
        for start in range(0, len(df), chunk_size):
            yield df.iloc[start:start + chunk_size]
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the connection and return status"""
        try:
            self.connect()
            return {'success': True, 'message': 'Connection successful'}
        except Exception as e:
            return {'success': False, 'message': str(e)}
        finally:
            self.disconnect()
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
