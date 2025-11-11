from pathlib import Path
from pyspark.sql import DataFrame
import pyspark.sql.types as T
from abc import ABC, abstractmethod
from databricks.sdk.runtime import *
from dataclasses import dataclass


class BasicLoaderWriter(ABC):
    """
    Abstract base class for loading and writing data to Unity catalog.
    Each layer (bronze, silver, gold) has different implementation.

    """

    @abstractmethod
    def read_data(self, file_path: Path | str, schema: T.StructType):
        """
        Read data from file to Spark DataFrame.

        Args:
            file_path (Path|str): Path to raw file.
            schema (T.StructType): Pyspark schema object.
        """
        pass

    @abstractmethod
    def write_data(self, spdf: DataFrame, mode: str, uc_table_nm: str):
        """
        Write data to unity catalog.

        Args:
            spdf (DataFrame): Spark Dataframe.
            mode (str): Write mode. Allowed options overwrite/append.
            uc_table_nm (str): Unity catalog table name. Allowed format: catalog.schema.table.
        """
        pass


class BronzeLoaderWriter(BasicLoaderWriter):
    """
    Class for loading and writing data to bronze layer.
    """

    def __init__(self, config: dataclass) -> None:
        self.config = config

    def read_data(self, file_path: Path | str, schema: T.StructType) -> DataFrame:
        """
        Read raw data in format .csv.

        Args:
            spark: SparkSession.
            file_path (Path|str): Path to raw file.
            schema (T.StructType): Pyspark schema object.

        Returns (DataFrame): Pyspark dataframe.
        """
        return spark.read.csv(
            file_path,
            header=True,
            inferSchema=True,
            multiLine=True,
            schema=schema,
        )

    def write_data(self, spdf: DataFrame, mode: str, uc_table_nm: str) -> None:
        """
        Writes spark dataframe to Unity catalog.
        """
        (spdf.write.mode(mode).saveAsTable(uc_table_nm))



