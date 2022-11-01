import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ByteType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.utils.spark_utils import spark_utils


class RxClaims:
    """
    Pharmacy Claims data class containing respective ETL logic
    """

    def __init__(self):
        self.spark = spark_utils.spark_session

        self.path = "sample-data/pharmacy_claims/*"
        self.header = True
        self.key_columns = ["card_id", "claim_number", "fill_date", "ndc_11"]

    @property
    def schema(self):
        return StructType(
            [
                StructField(
                    name="allowed_amount", dataType=DecimalType(5, 4), nullable=False
                ),
                StructField(
                    name="card_id",
                    dataType=StringType(),
                    nullable=False),
                StructField(
                    name="claim_line_number", dataType=ByteType(), nullable=False
                ),
                StructField(
                    name="claim_status",
                    dataType=StringType(),
                    nullable=False),
                StructField(
                    name="days_supply",
                    dataType=IntegerType(),
                    nullable=False),
                StructField(
                    name="fill_date",
                    dataType=DateType(),
                    nullable=True),
                StructField(
                    name="maintenance_drug_flag", dataType=StringType(), nullable=False
                ),
                StructField(
                    name="medication_name", dataType=StringType(), nullable=False
                ),
                StructField(
                    name="ndc_11",
                    dataType=StringType(),
                    nullable=False),
                StructField(
                    name="paid_date",
                    dataType=DateType(),
                    nullable=False),
                StructField(
                    name="pharmacy_id",
                    dataType=StringType(),
                    nullable=True),
                StructField(
                    name="pharmacy_name", dataType=StringType(), nullable=False
                ),
                StructField(
                    name="pharmacy_npi",
                    dataType=StringType(),
                    nullable=False),
                StructField(
                    name="pharmacy_tax_id", dataType=StringType(), nullable=False
                ),
                StructField(
                    name="prescriber_id",
                    dataType=LongType(),
                    nullable=False),
                StructField(
                    name="prescriber_npi", dataType=StringType(), nullable=True
                ),
                StructField(
                    name="quantity_dispensed", dataType=DecimalType(), nullable=False
                ),
                StructField(
                    name="refill_number", dataType=IntegerType(), nullable=False
                ),
                StructField(
                    name="retail_mail_flag", dataType=StringType(), nullable=False
                ),
                StructField(
                    name="run_date",
                    dataType=DateType(),
                    nullable=False),
                StructField(
                    name="rxtype",
                    dataType=StringType(),
                    nullable=False),
                StructField(
                    name="specialty_drug_flag", dataType=StringType(), nullable=False
                ),
                StructField(
                    name="strength_units", dataType=StringType(), nullable=False
                ),
                StructField(
                    name="strength_value", dataType=DecimalType(), nullable=False
                ),
                StructField(
                    name="uploadDate",
                    dataType=DateType(),
                    nullable=False),
                StructField(
                    name="claim_number",
                    dataType=StringType(),
                    nullable=False),
            ]
        )

    def fetch(self):
        self.df = self.spark.read.csv(
            path=self.path, header=self.header, schema=self.schema
        )

    def clean(self):
        """
        converts `claim_status` column to uppercase
        """
        self.df = self.df.withColumn(
            "claim_status", F.upper(
                F.col("claim_status")))

    def transform(self):
        """
        removes `denied` claims, merges `paid` and `reversal` claims, filters out zero `days_supply` records
        and converts ndc 11 to ndc 9 codes
        """
        denied_claim = F.col("claim_status") == F.lit("DENIED")
        has_days_supply = F.col("days_supply") > 0

        self.df = (
            self.df.filter(~denied_claim)
            .groupBy(self.key_columns)
            .agg(
                F.first("claim_status").alias("claim_status"),
                F.sum("days_supply").alias("days_supply"),
                F.sum("allowed_amount").alias("allowed_amount"),
            )
            .filter(has_days_supply)
            .withColumn("ndc_9", F.expr("substring(ndc_11, 1, length(ndc_11)-2)"))
        )

    @property
    def output(self) -> DataFrame:
        return self.df

    def run(self):
        print("running RxClaims ETL")
        self.fetch()
        self.clean()
        self.transform()
