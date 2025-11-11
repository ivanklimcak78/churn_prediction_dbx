from dataclasses import dataclass, field 
import pyspark.sql.types as T

@dataclass
class ConfigBronze:
    """
    Dataclass for bronze config.

    Args:
        file_path (str): Path to raw .csv file.
        write_mode (str): Spark mode. Allowed values are overwrite and merge.
        bronze_table_uc_nm (str): Name of bronze table.
        schema (StructType): Spark schema.
    """

    file_path: str
    write_mode: str
    bronze_table_uc_nm: str
    schema: T.StructType = T.StructType()

@dataclass
class ConfigSilver:
    """
    Dataclass for silver config.

    Args:
        bronze_table_uc_nm: Name of bronze table.
        write_mode (str): Spark mode. Allowed values are overwrite and merge.
        silver_table_uc_nm (str): Name of silver table.
        schema (T.StructType): Spark schema.
    """

    bronze_table_uc_nm: str
    write_mode: str
    silver_table_uc_nm: str
    schema: T.StructType = T.StructType()

@dataclass 
class ConfigFeatureEngineering:
    """
    Dataclass for feature engineering config.

    Args:
        id_cols (list): List of identifiers columns names.
        label_col (list): List of label column name.
        split_col (list): List of split columns names.
        internet_cols (list): List of internet services columns names.
        internet_values_map (dict): Dictionary mapping internet columns to values.
        internet_services_cnt_nm (str): Name of total internet services column (new feature).
        fill_missing_cols (list): List of columns to fill missing values (expert imputation).
    """
    id_cols: list
    label_col: list
    split_col: list 
    internet_cols: list = field(default_factory=list)
    internet_values_map: dict = field(default_factory=dict)
    internet_services_cnt_nm: str = ""
    fill_missing_cols: list = field(default_factory=list)

@dataclass 
class ConfigTraining:
    """
    Dataclass for training.
    """
    train_valid_test_rt: list 
    seed: int
    features_table_nm: str 
    labels_table_nm: str


@dataclass
class CustomerChurnSchemas:
    """
    Dataclass containing all customer churn data schemas.
    """

    @property 
    def bronze_schema(self) -> T.StructType:
        """
        Return Bronze customer churn data schema.
        """
        return T.StructType(
            [
                T.StructField("customerID", T.StringType(), False),
                T.StructField("gender", T.StringType(), True),
                T.StructField("SeniorCitizen", T.DecimalType(2, 1), True),
                T.StructField("Partner", T.StringType(), True),
                T.StructField("Dependents", T.StringType(), True),
                T.StructField("tenure", T.DecimalType(18, 2), True),
                T.StructField("PhoneService", T.StringType(), True),
                T.StructField("MultipleLines", T.StringType(), True),
                T.StructField("InternetService", T.StringType(), True),
                T.StructField("OnlineSecurity", T.StringType(), True),
                T.StructField("OnlineBackup", T.StringType(), True),
                T.StructField("DeviceProtection", T.StringType(), True),
                T.StructField("TechSupport", T.StringType(), True),
                T.StructField("StreamingTV", T.StringType(), True),
                T.StructField("StreamingMovies", T.StringType(), True),
                T.StructField("Contract", T.StringType(), True),
                T.StructField("PaperlessBilling", T.StringType(), True),
                T.StructField("PaymentMethod", T.StringType(), True),
                T.StructField("MonthlyCharges", T.DecimalType(18, 2), True),
                T.StructField("TotalCharges", T.DecimalType(18, 2), True),
                T.StructField("Churn", T.StringType(), True),
            ]
        )
