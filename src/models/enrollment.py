import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, StringType, StructField, StructType

from src.utils.spark_utils import spark_utils


class Enrollment:
    """
    Enrollment data class containing respective ETL logic
    """

    def __init__(self):
        self.spark = spark_utils.spark_session

        self.path = "sample-data/enrollment.csv"
        self.header = True

    @property
    def schema(self):
        return StructType(
            [
                StructField(
                    name="birth_date",
                    dataType=DateType(),
                    nullable=False),
                StructField(
                    name="card_id",
                    dataType=StringType(),
                    nullable=False),
                StructField(
                    name="enrollment_end_date", dataType=DateType(), nullable=False
                ),
                StructField(
                    name="enrollment_start_date", dataType=DateType(), nullable=False
                ),
                StructField(
                    name="first_name",
                    dataType=StringType(),
                    nullable=False),
                StructField(
                    name="gender",
                    dataType=StringType(),
                    nullable=True),
                StructField(
                    name="last_name",
                    dataType=StringType(),
                    nullable=False),
            ]
        )

    def fetch(self):
        self.df = self.spark.read.csv(
            path=self.path, header=self.header, schema=self.schema
        )

    def transform(self):
        """
        remove "ID" from `card_id` column
        """
        self.df = self.df.withColumn(
            "card_id", F.regexp_replace(F.upper(F.col("card_id")), "ID", "")
        )

    @property
    def output(self) -> DataFrame:
        return self.df

    def run(self):
        print("running Enrollment ETL")
        self.fetch()
        self.transform()
