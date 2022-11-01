import pyspark.sql.functions as F

from src.models.enrollment import Enrollment
from src.models.ndc9_lookup import NDC9Lookup
from src.models.rxclaims import RxClaims
from src.utils.json_utils import output_patient_records_as_json


def build_patient_summary():
    """
    Builds and outputs patient medicine summary as JSON files
    """
    enrollment = Enrollment()
    rxclaims = RxClaims()
    ndc9_lookup = NDC9Lookup()

    # run ETL for each model
    enrollment.run()
    rxclaims.run()
    ndc9_lookup.run()

    print("creating patient medicine summaries")
    # only include patients enrolled as of July 1st, 2020
    enrollment_july_2020 = enrollment.output.filter(
        (F.col("enrollment_start_date") < F.lit("2020-07-01"))
        & (F.col("enrollment_end_date") >= F.lit("2020-07-01"))
    )

    # filter rx between Jan 1st, 2020 and June 30th, 2020
    rxclaims_jan_to_july = rxclaims.output.filter(
        F.col("fill_date").between(F.lit("2020-01-01"), F.lit("2020-06-30"))
    )

    # only include patients that have claims data and vice-versa
    enrollment_claims = enrollment_july_2020.join(
        rxclaims_jan_to_july, on="card_id", how="inner"
    )

    # map ndc11 to ndc9 generic names
    enrollment_claim_meds = enrollment_claims.join(
        ndc9_lookup.df_output, on="ndc_9", how="left"
    )

    # data check: number of mappings not found
    print(
        f'{enrollment_claim_meds.where(F.col("generic_name").isNull()).count()} '
        f"codes in rxclaims did not find a match in NDC9 mapping"
    )

    # select, sum and agg columns for output
    output_df = (
        enrollment_claim_meds.where(F.col("generic_name").isNotNull())
        .groupBy("card_id", "first_name", "last_name", "generic_name")
        .agg(
            F.sum("allowed_amount").alias("allowed_amount"),
            F.sum("days_supply").alias("days_supply"),
        )
    )

    output_df = (
        output_df.groupBy("first_name", "last_name", "card_id")
        .agg(
            F.collect_list(F.struct("allowed_amount", "days_supply")).alias(
                "med_summary"
            ),
            F.collect_list(F.col("generic_name")).alias("generic_names"),
        )
        .select(
            "card_id",
            "first_name",
            "last_name",
            F.map_from_arrays(F.col("generic_names"), F.col("med_summary")).alias(
                "med_summary"
            ),
        )
    )

    output_patient_records_as_json(
        df=output_df, output_path="output-data/patient_records/"
    )


if __name__ == "__main__":
    build_patient_summary()
