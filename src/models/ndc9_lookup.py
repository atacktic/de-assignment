import json

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from src.utils.spark_utils import spark_utils


class NDC9Lookup:
    """
    NDC9 lookup data class containing respective ETL logic
    """

    def __init__(self):
        self.path = "sample-data/ndc9_lookup.json"
        self.df_output_schema = StructType(
            [
                StructField(
                    name="ndc_9",
                    dataType=StringType(),
                    nullable=False),
                StructField(
                    name="generic_name",
                    dataType=StringType(),
                    nullable=False),
            ]
        )

    def fetch(self):
        self.ndc9_lookup_data = json.load(open(self.path))

    def transform(self):
        """
        converts json records to dictionary and lists containing ndc9 code and generic name
        """
        ndc9s = {}
        df_data = []
        for code in self.ndc9_lookup_data:
            ndc9s[code] = self.ndc9_lookup_data[code]["genericName"]
            df_data.append(
                [
                    self.ndc9_lookup_data[code]["id"],
                    self.ndc9_lookup_data[code]["genericName"],
                ]
            )

        self.ndc9s = ndc9s
        self.ndc9_df_data = df_data

    @property
    def dict_output(self) -> dict:
        """
        returns data as key: value pairs
        """
        return self.ndc9s

    @property
    def df_output(self) -> DataFrame:
        """
        returns data as spark DF
        """
        self.spark = spark_utils.spark_session
        return self.spark.createDataFrame(
            self.ndc9_df_data, self.df_output_schema)

    def run(self):
        print("running NDC9Lookup ETL")
        self.fetch()
        self.transform()
