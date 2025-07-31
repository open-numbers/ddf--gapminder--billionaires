#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hurun Data Transformation Script

This script transforms Hurun billionaire data from various years into a consistent format,
addressing issues identified during analysis and implementing the methodology defined in
the project documentation.

Key tasks:
1. Handle different formats across years (pre-2019 vs post-2019)
2. Standardize wealth values
3. Create consistent person identification
4. Extract and standardize country information
5. Handle duplicate people
6. Merge data into a unified DDF structure
"""

import glob
import os
import re
import unicodedata
from collections import defaultdict

import numpy as np
import pandas as pd


def load_hurun_data(source_dir="../source/hurun"):
    """Load all Hurun data files into a dictionary with year as key."""
    hurun_data = {}
    for file_path in glob.glob(f"{source_dir}/*.csv"):
        year = int(os.path.basename(file_path).replace(".csv", ""))
        hurun_data[year] = pd.read_csv(file_path, na_values=["未知"], keep_default_na=True)
        print(f"Loaded {year} data: {hurun_data[year].shape}")
    return hurun_data


def to_concept_id(name):
    """Convert a name to a slug/concept_id format."""
    if not isinstance(name, str):
        return name

    # Convert to lowercase
    name = name.lower().strip()

    # Remove accents
    name = unicodedata.normalize("NFKD", name)
    name = "".join([c for c in name if not unicodedata.combining(c)])

    # Replace special characters and spaces with underscore
    name = re.sub(r"[^\w\s]", "_", name)
    name = re.sub(r"\s+", "_", name)

    # Remove consecutive underscores
    name = re.sub(r"_+", "_", name)

    # Remove leading/trailing underscores
    name = name.strip("_")

    return name


def extract_wealth(x):
    """Extract wealth in Billion from input value

    input value can be form of:

    - $ 10000 M
    - $ 10000 Million
    - $ 1.0  <- if no unit, assume it's Billion
    """
    if not isinstance(x, str):
        return np.nan

    # Extract numeric value
    match = re.search(r"(\d+)", x)
    if not match:
        return np.nan

    value = float(match.group(1))

    # Convert to billions based on unit
    if "M" in x:
        return value / 1000  # Convert millions to billions
    elif "B" in x:
        return value
    else:
        return value  # Assume billion if no unit specified


def standardize_wealth_values(hurun_data):
    """Standardize wealth values across different year formats."""
    transformed_data = {}

    # Process by year range
    for year, df in sorted(hurun_data.items()):
        print(f"Standardizing wealth values for {year}...")
        transformed_df = df.copy()

        # 2012-2016: String format like "$ 55000 Million"
        if year <= 2016:
            if "Wealth" in df.columns:
                wealth_col = "Wealth"
            elif "Wealth(US$)" in df.columns:
                wealth_col = "Wealth(US$)"
            else:
                print(f"  Warning: No wealth column found for {year}")
                transformed_data[year] = transformed_df
                continue
            # Extract numeric value and convert to billions
            transformed_df["wealth_billion"] = transformed_df[wealth_col].apply(extract_wealth)

        # 2017+: Simple numeric values
        elif year >= 2017:
            # 2017-2018
            if "Wealth" in df.columns:
                # These appear to be already in billions
                transformed_df["wealth_billion"] = transformed_df["Wealth"]
            # 2019
            elif "hs_Rank_Global_Wealth_USD" in df.columns:
                # This appears to be in billions already
                transformed_df["wealth_billion"] = transformed_df["hs_Rank_Global_Wealth_USD"]
            else:
                print(f"  Warning: No wealth column found for {year}")

        transformed_data[year] = transformed_df

    return transformed_data


def extract_country_info(hurun_data):
    """Extract and standardize country information."""
    transformed_data = {}

    for year, df in sorted(hurun_data.items()):
        print(f"Extracting country info for {year}...")
        transformed_df = df.copy()

        # Helper function to extract country from location string
        def extract_country(value):
            if pd.isnull(value) or not isinstance(value, str):
                return np.nan

            # Special cases for regions
            if "Hong Kong" in value:
                return "Hong Kong"
            if "Taiwan" in value or "Taipei" in value:
                return "Taiwan"
            if "Singapore" in value:
                return "Singapore"

            # Normal case: Country is the first part before any dash
            parts = value.split("-")
            country = parts[0].strip()

            # Manual mapping for known problematic values
            country_map = {
                # China provinces/cities
                "Anhui": "China",
                "Hebei": "China",
                "Guangdong": "China",
                "Sichuan": "China",
                "Hunan": "China",
                "Tibet": "China",
                "Shanxi": "China",
                "Shandong": "China",
                "Yunnan": "China",
                "Jiangsu": "China",
                "Henan": "China",
                "Beijing": "China",
                "Shanghai": "China",
                "ChinaChangsha": "China",
                "Fujian": "China",
                "Hubei": "China",
                "Nei Mongol": "China",
                "Zhejiang": "China",
                # India cities
                "Vadodara": "India",
                "Chandigarh": "India",
                "Kolkata": "India",
                "Bikaner": "India",
                "Kozhikode": "India",
                "Bengaluru": "India",
                "New Delhi": "India",
                "Hyderabad": "India",
                "Ahmedabad": "India",
                "Chennai": "India",
                # US locations
                "Santa Clara": "United States",
                # Other malformed
                "United StatesNew Jersey": "United States",
            }

            if country in country_map:
                return country_map[country]

            # If no dash, it might be a valid country or a warning case
            if len(parts) < 2:
                print(f"WARNING: country not in correct format - {value}")

            # Fix common issues
            if country == "USA":
                return "United States"
            if country == "UAE":
                return "United Arab Emirates"
            if country == "UK":
                return "United Kingdom"

            return country

        # For 2019+, use hs_Character_Permanent_En as primary source
        if year >= 2019:
            # Extract country from permanent residence
            if "hs_Character_Permanent_En" in df.columns:
                transformed_df["country"] = transformed_df["hs_Character_Permanent_En"].apply(
                    extract_country
                )

            # Use BirthPlace as last resort
            mask = transformed_df["country"].isnull()
            if "hs_Character_BirthPlace_En" in df.columns and mask.any():
                transformed_df.loc[mask, "country"] = transformed_df.loc[
                    mask, "hs_Character_BirthPlace_En"
                ].apply(extract_country)

        # For earlier years, we'll leave country as null
        # as per methodology document, we'll need to impute this later
        else:
            transformed_df["country"] = np.nan

        transformed_data[year] = transformed_df

    return transformed_data


def standardize_company_info(hurun_data):
    """Standardize company information across different year formats."""
    transformed_data = {}

    for year, df in sorted(hurun_data.items()):
        print(f"Standardizing company info for {year}...")
        transformed_df = df.copy()

        # Add standardized company column
        if year <= 2016:
            company_col = "Companies"
        elif 2017 <= year <= 2018:
            company_col = "CNameEn"
        else:
            company_col = "hs_Rank_Global_ComName_En"

        if company_col in df.columns:
            transformed_df["company"] = transformed_df[company_col]
        else:
            transformed_df["company"] = np.nan

        transformed_data[year] = transformed_df

    return transformed_data


def standardize_industry_info(hurun_data):
    """Standardize industry information across different year formats."""
    transformed_data = {}

    for year, df in sorted(hurun_data.items()):
        print(f"Standardizing industry info for {year}...")
        transformed_df = df.copy()

        # Add standardized industry column
        if year <= 2016:
            industry_col = "Industry"
        elif 2017 <= year <= 2018:
            industry_col = "IndustryEn"
        else:
            industry_col = "hs_Rank_Global_Industry_En"

        if industry_col in df.columns:
            transformed_df["industry"] = transformed_df[industry_col]
        else:
            transformed_df["industry"] = np.nan

        transformed_data[year] = transformed_df

    return transformed_data


def standardize_person_name_id(hurun_data):
    """Standardize person information and create consistent identification."""
    transformed_data = {}
    name_columns = {
        # Define the name column for each year range
        "2012-2016": "Name",
        "2017-2018": "NameEn",
        "2019+": "hs_Character_Fullname_En",
    }
    chinese_name_columns = {
        # Define the Chinese name column for each year range
        "2019+": "hs_Character_Fullname_Cn"
    }

    for year, df in sorted(hurun_data.items()):
        print(f"Standardizing person info for {year}...")
        transformed_df = df.copy()

        # Determine which name column to use
        if year <= 2016:
            name_col = name_columns["2012-2016"]
        elif 2017 <= year <= 2018:
            name_col = name_columns["2017-2018"]
        else:
            name_col = name_columns["2019+"]

        # Verify the column exists
        if name_col not in df.columns:
            print(f"  Warning: Name column '{name_col}' not found for {year}")
            transformed_data[year] = transformed_df
            continue

        # Generate person_id from name
        # Note: We'll need to handle duplicates later
        transformed_df["name"] = transformed_df[name_col]

        # Add Chinese name if available
        if year >= 2019:
            chinese_name_col = chinese_name_columns["2019+"]
            if chinese_name_col in df.columns:
                transformed_df["chinese_name"] = transformed_df[chinese_name_col]
            else:
                transformed_df["chinese_name"] = np.nan
        else:
            transformed_df["chinese_name"] = np.nan

        # Clean name: remove family/brothers suffixes for ID generation
        def clean_name_for_id(name):
            if not isinstance(name, str):
                return name

            name = name.strip()
            name = re.sub(
                r" family$| brothers$| and family$|&family$|& family$",
                "",
                name,
                flags=re.IGNORECASE,
            )
            return name

        transformed_df["clean_name"] = transformed_df["name"].apply(clean_name_for_id)
        transformed_df["person_id"] = transformed_df["clean_name"].apply(to_concept_id)

        # Store original info for debugging
        transformed_df["original_id"] = ""
        if "ID" in df.columns:
            transformed_df["original_id"] = df["ID"]
        elif "hs_Character_ID" in df.columns:
            transformed_df["original_id"] = df["hs_Character_ID"]

        transformed_data[year] = transformed_df

    return transformed_data


def get_birth_year(df):
    """Extract birth year from various columns."""
    if "hs_Character_Birthday" in df.columns:

        def extract_year(date_str):
            if not isinstance(date_str, str):
                return np.nan
            try:
                return int(date_str.split("-")[0])
            except ValueError:
                return np.nan

        return df["hs_Character_Birthday"].apply(extract_year)

    elif "Birth" in df.columns:
        # Some years have birth year directly
        return pd.to_numeric(df["Birth"], errors="coerce")

    elif "hs_Character_Age" in df.columns:
        # Calculate from age and year
        def calc_birth_year(row):
            if pd.isnull(row["hs_Character_Age"]):
                return np.nan
            try:
                age = int(row["hs_Character_Age"])
                # Use the year from the dataframe if available, otherwise assume current processing year
                year = getattr(row, "year", 2020)  # fallback year
                return int(year) - age
            except ValueError:
                return np.nan

        return df.apply(calc_birth_year, axis=1)

    # No birth info available
    return pd.Series([np.nan] * len(df))


def add_birth_year_column(transformed_data):
    """Add birth_year column to all datasets."""
    print("Adding birth_year column...")

    for year, df in transformed_data.items():
        if "birth_year" not in df.columns:
            df["birth_year"] = get_birth_year(df)

    return transformed_data


def handle_duplicate_people(transformed_data):
    """
    Handle cases where multiple people have the same name but are different individuals.
    Priority order: 1. original_id, 2. birth_year, 3. company, 4. sequential numbering
    """
    print("Handling duplicate people across years...")

    # Combine data from all years for analysis
    all_data = []
    for year, df in transformed_data.items():
        df_copy = df.copy()
        df_copy["year"] = year
        all_data.append(df_copy)

    if not all_data:
        print("  No data to process")
        return transformed_data

    combined = pd.concat(all_data, ignore_index=True)

    # Find person_ids that have duplicates within the same year
    duplicate_person_ids = set()
    for person_id, group in combined.groupby("person_id"):
        for year in group["year"].unique():
            year_group = group[group["year"] == year]
            if len(year_group) > 1:
                duplicate_person_ids.add(person_id)
                break

    # Process each duplicate person_id
    duplicate_map = {}
    for person_id in duplicate_person_ids:
        group = combined[combined["person_id"] == person_id]
        print(f"  Found duplicates for: {person_id}")

        # Determine distinguishing feature in priority order
        distinguishing_col = None
        if "original_id" in group.columns and group["original_id"].nunique() > 1:
            distinguishing_col = "original_id"
        elif "birth_year" in group.columns and group["birth_year"].nunique() > 1:
            distinguishing_col = "birth_year"
        elif "company" in group.columns and group["company"].nunique() > 1:
            distinguishing_col = "company"

        if distinguishing_col:
            # Use the distinguishing column to create unique IDs
            unique_values = group[distinguishing_col].dropna().astype(str).sort_values().unique()
            for i, value in enumerate(unique_values):
                if i > 0:  # Skip the first (keeps original ID)
                    new_id = f"{person_id}_{i + 1}"
                    print(f"    Assigning {new_id} to records with {distinguishing_col}={value}")
                    duplicate_map[(person_id, distinguishing_col, value)] = new_id
        else:
            # Sequential numbering as last resort
            print(f"    Using sequential numbering for {person_id}")
            # Group by year and assign sequential numbers within each year
            for year in group["year"].unique():
                year_group = group[group["year"] == year]
                if len(year_group) > 1:
                    # Sort for consistency
                    year_group = year_group.sort_values(["name", "original_id"])
                    for i, _ in enumerate(year_group.iterrows()):
                        if i > 0:  # Skip the first record
                            new_id = f"{person_id}_{i + 1}"
                            print(f"    Assigning {new_id} to record in year {year}")
                            # Use year and index as key
                            duplicate_map[(person_id, "sequential", f"{year}_{i}")] = new_id

    # Apply the duplicate mapping to each year's data
    for year, df in transformed_data.items():
        print(f"  Applying duplicate handling to {year}...")

        for (old_id, method, key), new_id in duplicate_map.items():
            if method == "original_id":
                mask = (df["person_id"] == old_id) & (df["original_id"].astype(str) == key)
            elif method == "birth_year":
                mask = (df["person_id"] == old_id) & (df["birth_year"].astype(str) == key)
            elif method == "company":
                mask = (df["person_id"] == old_id) & (df["company"].astype(str) == key)
            elif method == "sequential":
                # For sequential, key is "year_index"
                parts = key.split("_")
                if len(parts) == 2 and parts[0] == str(year):
                    # Find the nth occurrence in this year
                    year_mask = df["person_id"] == old_id
                    year_records = df[year_mask].sort_values(["name", "original_id"])
                    if len(year_records) > int(parts[1]):
                        idx = year_records.iloc[int(parts[1])].name
                        mask = df.index == idx
                    else:
                        continue
                else:
                    continue
            else:
                continue

            if mask.any():
                df.loc[mask, "person_id"] = new_id

    return transformed_data


def create_unified_dataset(transformed_data):
    """Create a unified dataset with consistent structure."""
    print("Creating unified dataset...")

    # Key columns to extract
    key_columns = [
        "person_id",
        "name",
        "chinese_name",
        "year",
        "wealth_billion",
        "country",
        "original_id",
    ]

    # Combine data from all years
    all_data = []
    for _, df in sorted(transformed_data.items()):
        year_df = df.copy()

        # Ensure all key columns exist
        for col in key_columns:
            if col not in year_df.columns:
                year_df[col] = np.nan

        # Extract birth year
        year_df["birth_year"] = get_birth_year(year_df)

        # Set placeholder birth years (1900) to NA as they're likely not accurate
        year_df.loc[year_df["birth_year"] == 1900, "birth_year"] = np.nan

        # Add gender if available
        if "Sex" in year_df.columns:
            year_df["gender"] = year_df["Sex"]
        elif "hs_Character_Gender_Lang" in year_df.columns:
            year_df["gender"] = year_df["hs_Character_Gender_Lang"]
        else:
            year_df["gender"] = np.nan

        # Add headquarter if available
        if "hs_Rank_Global_ComHeadquarters_En" in year_df.columns:
            year_df["headquarter"] = year_df["hs_Rank_Global_ComHeadquarters_En"]
        else:
            year_df["headquarter"] = np.nan

        # Select the columns we want
        final_cols = [
            "person_id",
            "name",
            "chinese_name",
            "year",
            "wealth_billion",
            "country",
            "birth_year",
            "gender",
            "industry",
            "company",
            "headquarter",
            "original_id",
        ]

        year_df = year_df[final_cols]
        all_data.append(year_df)

    if not all_data:
        print("  No data to combine")
        return None

    # Combine all years
    combined = pd.concat(all_data, ignore_index=True)

    # Replace "未知" (unknown) with NaN
    combined = combined.replace("未知", np.nan)

    # If birth year and gender are both missing, nullify country
    mask = combined["birth_year"].isnull() & combined["gender"].isnull()
    combined.loc[mask, "country"] = np.nan

    # Handle duplicated person-year combinations
    dupes = combined.duplicated(subset=["person_id", "year"], keep=False)
    if dupes.any():
        print(f"  Found {dupes.sum()} duplicated person-year combinations")

        # For duplicates, keep the record with non-null wealth
        combined = combined.sort_values("wealth_billion", na_position="last")
        combined = combined.drop_duplicates(subset=["person_id", "year"], keep="first")

    return combined


def create_datapoint_entities(combined_data, output_dir="../intermediate/hurun"):
    """Create DDF datapoints and entities files."""
    print("Creating DDF files...")

    # Create directories if they don't exist
    os.makedirs(output_dir, exist_ok=True)

    # 1. Create datapoints: wealth by person and year
    datapoints = combined_data[["person_id", "year", "wealth_billion"]].copy()
    datapoints = datapoints.rename(columns={"person_id": "person", "wealth_billion": "wealth"})
    # Drop rows with missing wealth or person_id
    datapoints = datapoints.dropna(subset=["wealth", "person"])

    # Save datapoints
    datapoints_file = os.path.join(output_dir, "ddf--datapoints--wealth--by--person--year.csv")
    datapoints.sort_values(by=["person", "year"]).to_csv(datapoints_file, index=False)
    print(f"  Saved datapoints to {datapoints_file}")

    # 2. Create entities: person
    # Group by person_id and combine information across years
    person_groups = combined_data.groupby("person_id")

    person_entities = []
    for person_id, group in person_groups:
        # Start with the most recent record as base
        recent = group.sort_values("year", ascending=False).iloc[0]

        entity = {
            "person": person_id,
            "name": recent["name"],
            "chinese_name": recent["chinese_name"],
            "gender": recent["gender"],
            "birth_year": recent["birth_year"],
            "country": recent["country"],
            "industry": recent["industry"],
            "company": recent["company"],
            "headquarter": recent["headquarter"],
        }

        person_entities.append(entity)

    # Convert to DataFrame
    entities_df = pd.DataFrame(person_entities)

    # Sort by person ID
    entities_df = entities_df.sort_values(by="person")

    # Save entities
    entities_file = os.path.join(output_dir, "ddf--entities--person.csv")
    entities_df.to_csv(entities_file, index=False)
    print(f"  Saved entities to {entities_file}")

    return datapoints, entities_df


def process_hurun_data():
    """Main function to process all Hurun data."""
    print("=== Hurun Data Transformation ===")

    # Load data
    hurun_data = load_hurun_data()

    # Step 1: Standardize wealth values
    transformed_data = standardize_wealth_values(hurun_data)

    # Step 2: Extract country information
    transformed_data = extract_country_info(transformed_data)

    # Step 3: Standardize company information
    transformed_data = standardize_company_info(transformed_data)

    # Step 4: Standardize industry information
    transformed_data = standardize_industry_info(transformed_data)

    # Step 5: Standardize person information
    transformed_data = standardize_person_name_id(transformed_data)

    # Step 6: Add birth year column
    transformed_data = add_birth_year_column(transformed_data)

    # Step 7: Handle duplicate people
    transformed_data = handle_duplicate_people(transformed_data)

    # Step 8: Create unified dataset
    combined_data = create_unified_dataset(transformed_data)

    # Step 9: Create DDF files
    if combined_data is not None:
        datapoints, entities = create_datapoint_entities(combined_data)
        print(f"Created {len(datapoints)} datapoints and {len(entities)} person entities")

    print("=== Transformation Complete ===")


if __name__ == "__main__":
    process_hurun_data()
