from pyspark.sql import DataFrame
from delta.tables import DeltaTable
import logging
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

class DeltaOperations:
    def __init__(self, spark):
        self.spark = spark
    
    def write_delta_table(self, df: DataFrame, path: str, mode: str = "append", 
                         partition_by: list = None, merge_key: str = None):
        """Write DataFrame to Delta table with various options"""
        
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.save(path)
        logger.info(f"Successfully wrote data to {path}")
        
        return path
    
    def merge_delta_table(self, source_df: DataFrame, target_path: str, 
                         merge_condition: str, update_set: dict = None, 
                         insert_values: dict = None):
        """Perform merge (upsert) operation on Delta table"""
        
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            # If table doesn't exist, create it
            source_df.write.format("delta").save(target_path)
            return
        
        delta_table = DeltaTable.forPath(self.spark, target_path)
        
        merge_builder = delta_table.alias("target").merge(
            source_df.alias("source"), 
            merge_condition
        )
        
        if update_set:
            merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()
        
        if insert_values:
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_values)
        else:
            merge_builder = merge_builder.whenNotMatchedInsertAll()
        
        merge_builder.execute()
        logger.info(f"Successfully merged data into {target_path}")
    
    def optimize_table(self, table_path: str, z_order_by: list = None):
        """Optimize Delta table with optional Z-ordering"""
        
        if not DeltaTable.isDeltaTable(self.spark, table_path):
            logger.warning(f"Path {table_path} is not a Delta table")
            return
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        if z_order_by:
            delta_table.optimize().executeZOrderBy(*z_order_by)
            logger.info(f"Optimized table {table_path} with Z-order by {z_order_by}")
        else:
            delta_table.optimize().executeCompaction()
            logger.info(f"Optimized table {table_path} with compaction")
    
    def vacuum_table(self, table_path: str, retention_hours: int = 168):
        """Vacuum Delta table to remove old files"""
        
        if not DeltaTable.isDeltaTable(self.spark, table_path):
            logger.warning(f"Path {table_path} is not a Delta table")
            return
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        delta_table.vacuum(retention_hours)
        logger.info(f"Vacuumed table {table_path} with retention {retention_hours} hours")
    
    def get_table_history(self, table_path: str, limit: int = 20):
        """Get Delta table history"""
        
        if not DeltaTable.isDeltaTable(self.spark, table_path):
            logger.warning(f"Path {table_path} is not a Delta table")
            return None
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        return delta_table.history(limit)

    def append_with_partitions(
        self,
        df: DataFrame,
        path: str,
        partition_by: Optional[Iterable[str]] = None,
        optimize: bool = False,
        optimize_zorder: Optional[Iterable[str]] = None,
        vacuum_hours: Optional[int] = None,
    ) -> str:
        """Append to a Delta table with optional partitioning and post-write maintenance."""

        writer = df.write.format("delta").mode("append")
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(path)
        part_msg = f" partitions={list(partition_by)}" if partition_by else ""
        logger.info("Appended DataFrame to %s%s", path, part_msg)

        if optimize:
            self.optimize_table(path, list(optimize_zorder) if optimize_zorder else None)
        if vacuum_hours:
            self.vacuum_table(path, retention_hours=vacuum_hours)

        return path