from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkUtils:
    def __init__(self, conf: SparkConf = None):
        self.conf = (
            conf
            if conf
            else (
                SparkConf()
                .setMaster("local[2]")
                .setAppName("project-local")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "1040m")
                .set("spark.driver.memory", "520m")
                .set("spark.sql.shuffle.partitions", "5")
            )
        )

    @property
    def spark_session(self) -> SparkSession:
        session = SparkSession.builder.config(conf=self.conf).getOrCreate()

        return session


spark_utils = SparkUtils()
