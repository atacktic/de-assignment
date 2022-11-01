from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import Row

from src.models.rxclaims import RxClaims
from src.utils.spark_utils import spark_utils


class TestRxClaims:
    spark = spark_utils.spark_session

    @pytest.fixture
    def mock_df(self):
        return self.spark.createDataFrame(
            [
                Row(
                    card_id="101",
                    claim_number="2000",
                    fill_date=date(2020, 4, 5),
                    ndc_11="421920101",
                    claim_status="DENIED",
                    days_supply=7,
                    allowed_amount=Decimal(10.2),
                ),
                Row(
                    card_id="101",
                    claim_number="2001",
                    fill_date=date(2020, 4, 6),
                    ndc_11="721920559",
                    claim_status="PAID",
                    days_supply=28,
                    allowed_amount=Decimal(11.9),
                ),
                Row(
                    card_id="101",
                    claim_number="2001",
                    fill_date=date(2020, 4, 6),
                    ndc_11="721920559",
                    claim_status="REVERSAL",
                    days_supply=-28,
                    allowed_amount=Decimal(-11.9),
                ),
                Row(
                    card_id="101",
                    claim_number="2002",
                    fill_date=date(2020, 4, 7),
                    ndc_11="621920701",
                    claim_status="PAID",
                    days_supply=30,
                    allowed_amount=Decimal(12.0),
                ),
            ]
        )

    @pytest.fixture
    def expected_output_df(self):
        return self.spark.createDataFrame(
            [
                Row(
                    card_id="101",
                    claim_number="2002",
                    fill_date=date(2020, 4, 7),
                    ndc_11="621920701",
                    claim_status="PAID",
                    days_supply=30,
                    allowed_amount=Decimal(12),
                    ndc_9="6219207",
                ),
            ]
        )

    def test_clean_transform(self, mock_df, expected_output_df):
        rxclaims = RxClaims()
        rxclaims.df = mock_df

        rxclaims.clean()
        rxclaims.transform()
        result = rxclaims.output

        assert result.toPandas().equals(expected_output_df.toPandas())
