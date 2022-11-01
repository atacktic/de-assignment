import json
import re

from pyspark.sql import DataFrame


def output_patient_records_as_json(df: DataFrame, output_path: str):
    """
    Writes each input dataframe patient record to a JSON file

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame containing agg of patient records
    output_path : string
        File path to output JSON records
    """
    print("writing patient record files")

    json_rows = df.toJSON().collect()

    for row in json_rows:
        # get patient ID from JSON string to use in file name
        id = re.findall(r"(?<=\"card_id\":\")(.*)(?=\",\"first_name)", row)[0]
        # remove ID from JSON output
        record = row.replace(f'"card_id":"{id}",', "")

        with open(f"{output_path}patient_ID{id}.json", "w", encoding="utf-8") as file:
            json.dump(record, file, indent=2, separators=(',', ': '))
