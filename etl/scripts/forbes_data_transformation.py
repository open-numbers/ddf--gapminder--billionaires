import pandas as pd
import os
import datetime


# Reference date is 2025-04-01. we assume this is the release date for latest forbes
# change it when new data comes.
reference_date = datetime.datetime(2025, 4, 1)


def transform_forbes_data(source_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_files = [os.path.join(source_dir, f) for f in os.listdir(source_dir) if f.endswith(".csv")]
    all_files.sort()

    datapoints_list = []
    entities_map = {}

    for file in all_files:
        year = int(os.path.basename(file).split(".")[0])
        df = pd.read_csv(file)

        # Per methodology, remove junk data where 'worth' is a floating-point number.
        # This is interpreted as removing rows where 'worth' is not a whole number.
        df["worth"] = pd.to_numeric(df["worth"], errors="coerce")
        df = df.dropna(subset=["worth"])
        df = df[df["worth"] % 1 == 0]

        df = df.rename(columns={"uri": "person"})

        for _, row in df.iterrows():
            if pd.isna(row["person"]):
                continue

            person_id = str(row["person"]).replace("-", "_")

            # Convert worth from millions to billions
            worth_in_billions = row["worth"] / 1000

            datapoints_list.append({"person": person_id, "year": year, "worth": worth_in_billions})

            image_uri = row.get("imageUri", "")

            # Calculate birth year from age (if age is available)
            age = row.get("age", "")
            birth_year = None
            if age and not pd.isna(age):
                try:
                    birth_year = reference_date.year - int(age)
                except (ValueError, TypeError):
                    birth_year = None

            # Overwrite with the latest data. Since files are sorted by year,
            # this will keep the data from the last year a person appears.
            entities_map[person_id] = {
                "person": person_id,
                "name": row.get("name", ""),
                "last_name": row.get("lastName", ""),
                "age": row.get("age", ""),
                "birth_year": birth_year,
                "gender": row.get("gender", ""),
                "country": row.get("country", ""),
                "source": row.get("source", ""),
                "industry": row.get("industry", ""),
                "title": row.get("title", ""),
                "image_uri": pd.NA if not image_uri.startswith("no-pic") else image_uri,
            }

    datapoints_df = pd.DataFrame(datapoints_list).dropna(subset=["worth"])
    entities_df = pd.DataFrame(list(entities_map.values()))

    # Sort data before saving
    datapoints_df = datapoints_df.sort_values(by=["person", "year"])
    entities_df = entities_df.sort_values(by="person")

    datapoints_df.to_csv(
        os.path.join(output_dir, "ddf--datapoints--worth--by--person--year.csv"),
        index=False,
    )
    entities_df.to_csv(os.path.join(output_dir, "ddf--entities--person.csv"), index=False)


if __name__ == "__main__":
    transform_forbes_data("../source/forbes", "../intermediate/forbes")
