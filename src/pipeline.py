from src.load import BronzeLoaderWriter
from src.config import BronzeConfig, SilverConfig


class pipeline():

    def __init__(self, bronze_config: BronzeConfig, silver_config: SilverConfig):
        self.bronze_config = bronze_config
        self.silver_config = silver_config
        self.feature_enginnering_config = feature_enginnering_config
        self.training_config = training_config
        self.pipeline_steps = pipeline_steps

    if bronze:



        bronze_raw_spdf = self.read_data(
            file_path=self.config.file_path,
            schema=self.config.schema,
        )

        self.write_data(
            spdf=bronze_raw_spdf,
            mode=self.config.write_mode,
            uc_table_nm=self.config.uc_table_nm,
        )



    def main(self) -> None:
        """
        Main script for silver layer creation.
        Contains following steps:
            1. Load bronze table.
            2. Apply data transformations.
            3. Create new features.
            4. Write to silver layer.

        """

        bronze_spdf = self.read_data(file_path=self.config.bronze_table_uc_nm)

        silver_spdf = self.transform_data(
            spdf=bronze_spdf,
        )

        silver_spdf = self.calculate_number_of_services(
            spdf=silver_spdf,
            values_to_map=self.config.values_to_map,
            cols_lst=self.config.cols_lst,
            col_nm=self.config.col_nm,
        )

        silver_spdf = self.

        self.write_data(
            spdf=silver_spdf,
            mode=self.config.write_mode,
            uc_table_nm=self.config.silver_table_uc_nm,
        )