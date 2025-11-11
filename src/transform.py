from load import BasicLoaderWriter
from itertools import chain
from pyspark.sql import DataFrame
from functools import reduce
import pyspark.sql.functions as F
import pyspark.sql.types as T
from dataclasses import dataclass
from databricks.sdk.runtime import *

class SilverLoaderWriter(BasicLoaderWriter):
    def __init__(self, config: dataclass) -> None:
        self.config = config

    def read_data(self, file_path: str, schema: T.StructType = None) -> DataFrame:
        """
        Load spark table from unity catalog.

        Args:
            file_path (str): Path to table in Unity Catalog.
            schema (T.StructType): Pyspark schema object.

        Returns (DataFrame): Pyspark dataframe.

        """
        return spark.table(file_path)

    def alter_types_spdf(self, spdf: DataFrame) -> DataFrame:
        """
        Alter datatypes for several columns from bronze layer.

        Args:
            spdf (DataFrame): Spark Dataframe with columns.

        Returns (DataFrame): Spark dataframe with altered datatypes.
        """

        return spdf.withColumns(
            {
                "SeniorCitizen": F.col("SeniorCitizen").cast(T.BooleanType()),
                "Partner": F.col("Partner").cast(T.BooleanType()),
                "Dependents": F.col("Dependents").cast(T.BooleanType()),
                "tenure": F.col("tenure").cast(T.IntegerType()),
                "PhoneService": F.col("PhoneService").cast(T.BooleanType()),
                "PaperlessBilling": F.col("PaperlessBilling").cast(T.BooleanType()),
                "Churn": F.col("Churn").cast(T.BooleanType()).cast(T.IntegerType()),
            }
        )

    def calculate_number_of_services(
        self, spdf: DataFrame, values_to_map: dict, cols_lst: list, col_nm: str
    ) -> DataFrame:
        """
        Calculate number of internet services, which user is actively using.

        Args:
            spdf (DataFrame): Spark Dataframe with columns.
            config (dict): Configuration dictionary.

        Returns (DataFrame): Spark dataframe with number of services.
        """

        mapping_expr = F.create_map([F.lit(x) for x in chain(*values_to_map.items())])
        cols_to_add = {
            "_".join([col, "int"]): F.coalesce(mapping_expr[F.col(col)], F.lit(0))
            for col in cols_lst
        }
        sum_expression = reduce(
            lambda a, b: a + b, [F.col(c) for c in list(cols_to_add.keys())]
        )

        return (
            spdf.withColumns(cols_to_add)
            .withColumn(col_nm, sum_expression)
            .drop(*list(cols_to_add.keys()))
        )

    def create_features(self,
        spdf: DataFrame, seed_nr: int, *args
    ) -> DataFrame:
        """
        Creates new featues for Spark DataFrame.

        Args:
            spdf (DataFrame): Spark Dataframe with columns.
            seed_nr (int): Seed number for random split.
            *args (float): Train, validate and test split ratios.
        
        Returns (DataFrame): Spark dataframe with new features.
        """

        monthly_increase = F.when(
            F.col("tenure") > 0,
            F.col("MonthlyCharges") - F.col("TotalCharges") / F.col("tenure"),
        ).otherwise(0)

        random = F.rand(seed=seed_nr)

        split = (
            F.when(random < args[0], "train")
            .when(random < (args[0] + args[1]), "validate")
            .otherwise("test")
        )

        return (
            spdf.withColumns(
                {
                    "transactions_ts": F.current_timestamp(),
                    "monthly_increase": monthly_increase.cast(T.DecimalType(18, 2)),
                    "random": random,
                }
            )
            .withColumn("split", split)
            .drop("random")
        )

    def impute_values(self, spdf: DataFrame, cols_to_fill: list) -> DataFrame:
        """
        Impute missing values for selected columns.
        
        Args:
            spdf (DataFrame): Spark dataframe.
            cols_to_fill (list): List of columns to impute.

        Returns:
            DataFrame: Spark dataframe with imputed values.
        """
        return (
            spdf.fillna(0, cols_to_fill)
        )
    
    def write_data(self, spdf: DataFrame, mode: str, uc_table_nm: str) -> None:
        """
        Write data to unity catalog.

        Args:
            spdf (DataFrame): Spark dataframe.
            mode (str): Write mode.
            uc_table_nm (str): Unity Catalog table name.
        """
        (
            spdf.write.mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(uc_table_nm)
        )

