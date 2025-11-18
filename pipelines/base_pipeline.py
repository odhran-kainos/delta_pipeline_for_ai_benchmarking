from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Any, Sequence
import yaml
import logging
from pathlib import Path

class BasePipeline(ABC):
    """Abstract base class for declarative pipelines"""
    
    def __init__(self, spark, config_path: str = None):
        self.spark = spark
        self.config = self.load_config(config_path) if config_path else {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load_config(self, config_path: str) -> Dict[Any, Any]:
        """Load pipeline configuration from YAML file"""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def require_config_keys(self, config: Dict[str, Any], required_paths: Sequence[str]) -> None:
        """Ensure required dotted configuration paths exist and are non-empty."""
        missing = []
        for path in required_paths:
            current: Any = config
            for segment in path.split('.'):
                if isinstance(current, dict) and segment in current:
                    current = current[segment]
                else:
                    missing.append(path)
                    break
            else:
                if current in (None, "", []):
                    missing.append(path)

        if missing:
            paths = ', '.join(sorted(set(missing)))
            raise ValueError(f"Missing required configuration keys: {paths}")
    
    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the data"""
        pass
    
    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """Load data to destination"""
        pass
    
    def run(self) -> None:
        """Execute the complete pipeline"""
        self.logger.info(f"Starting pipeline: {self.__class__.__name__}")
        
        # Extract
        raw_df = self.extract()
        self.logger.info("Data extraction completed")
        
        # Transform
        transformed_df = self.transform(raw_df)
        self.logger.info("Data transformation completed")
        
        # Load
        self.load(transformed_df)
        self.logger.info("Data loading completed")
        
        self.logger.info(f"Pipeline {self.__class__.__name__} completed successfully")
    
    def validate_data(self, df: DataFrame, stage: str) -> bool:
        """Basic data validation"""
        if df is None or df.count() == 0:
            self.logger.warning(f"No data found in {stage} stage")
            return False
        
        self.logger.info(f"{stage} stage validation passed - {df.count()} records")
        return True