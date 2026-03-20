import boto3
import csv
import io
import urllib.parse
from datetime import datetime

s3 = boto3.client("s3")


def lambda_handler(event, context):
    # 1. Figure out which bucket and file triggered the function
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    # SAFETY CHECK: Only process files dropped in the 'raw_data' folder!
    if "raw_data/" not in key:
        print("Not a raw file. Skipping to prevent infinite loops!")
        return "Skipped"

    try:
        # 2. Grab the raw CSV file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        raw_data = response["Body"].read().decode("utf-8")

        reader = csv.DictReader(io.StringIO(raw_data))

        # --- TRANSFORMATION 1: THE STANDARDIZER ---
        # Make all column headers lowercase and replace spaces with underscores
        original_headers = reader.fieldnames
        clean_headers = [
            header.strip().lower().replace(" ", "_") for header in original_headers
        ]

        # Add our brand new calculated column to the headers
        clean_headers.append("lead_time_days")

        processed_data = io.StringIO()
        writer = csv.DictWriter(processed_data, fieldnames=clean_headers)
        writer.writeheader()

        # Loop through the data row by row
        for row in reader:
            clean_row = {}
            # Map the old messy headers to our new clean headers
            for orig_key, clean_key in zip(original_headers, clean_headers[:-1]):
                clean_row[clean_key] = row[orig_key]

            # --- TRANSFORMATION 2: THE DATA SCRUBBER ---
            # If a row is missing the 'age' or 'distance', skip it entirely!
            if not clean_row.get("age") or not clean_row.get("distance_to_clinic_km"):
                continue

            # Convert 'Yes'/'No' no-shows to 1/0 for easy dashboard math
            if "no_show" in clean_row:
                if clean_row["no_show"].strip().lower() == "yes":
                    clean_row["no_show"] = 1
                elif clean_row["no_show"].strip().lower() == "no":
                    clean_row["no_show"] = 0

            # --- TRANSFORMATION 3: THE TIME TRACKER ---
            # Subtract booking date from appointment date
            try:
                # We split by 'T' just in case the data has timestamps (e.g., 2026-05-10T08:30:00)
                b_date_str = clean_row.get("scheduled_date", "").split("T")[0]
                a_date_str = clean_row.get("appointment_date", "").split("T")[0]

                if b_date_str and a_date_str:
                    # Convert the text into actual Python Dates
                    b_date = datetime.strptime(b_date_str, "%Y-%m-%d")
                    a_date = datetime.strptime(a_date_str, "%Y-%m-%d")

                    # Calculate the difference in days
                    lead_time = (a_date - b_date).days
                    clean_row["lead_time_days"] = lead_time
                else:
                    clean_row["lead_time_days"] = 0
            except Exception as e:
                clean_row["lead_time_days"] = 0

            writer.writerow(clean_row)

        # 5. Save the new, clean file into the processed-data folder
        new_key = key.replace("raw_data/", "cleaned_data/clean_")
        s3.put_object(Bucket=bucket, Key=new_key, Body=processed_data.getvalue())

        print(f"Success! Saved super-clean file to {new_key}")
        return "Data Engineering Pipeline Successful!"

    except Exception as e:
        print(f"Error processing the file: {e}")
        raise e
