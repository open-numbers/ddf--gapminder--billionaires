# Billionaire List Merging Process Example

This folder contains an example of the merging process for billionaire data from Hurun and Forbes lists, using the MCP name matching tool.

## Process Overview

1. We used the MCP name matcher tool to search for "Zong Qinghou" across both Hurun and Forbes datasets
2. The tool returned matches from both datasets with similarity scores
3. We analyzed the results and created ID mappings to merge entities

## Example Query Results

When searching for "Zong Qinghou", the MCP tool returned these key matches:

1. **Exact Matches (100% similarity)**:
   - Hurun: "Zong Qinghou" (person_id: zong_qinghou)
   - Forbes: "Zong Qinghou" (person_id: zong_qinghou)

2. **Related Entity (77% similarity)**:
   - Hurun: "Zong Qinghou family" (person_id: zong_qinghou_family)

## Merging Decision

As per the methodology document, we:
1. Identified that "Zong Qinghou" appears in both Hurun and Forbes datasets (exact match)
2. Determined that "Zong Qinghou family" from Hurun should be merged with the individual entity
3. Created a unified ID mapping that:
   - Uses "zong_qinghou" as the unified ID
   - Maps both Hurun entities ("zong_qinghou" and "zong_qinghou_family") to this unified ID
   - Maps the Forbes entity ("zong_qinghou") to this unified ID

## Data Discrepancies Noted

While not included in the ID mapping file itself, we observed these discrepancies:
- Birth year difference (1945 in Hurun vs 1948 in Forbes)
- Significant wealth value differences:
  - For Zong Qinghou (2021-2023): Hurun reports $15-19 billion vs Forbes $7.2-8.8 billion
  - For Elon Musk (2023-2025): Hurun reports $157-420 billion vs Forbes $180-342 billion
- The family entity in Hurun contains earlier wealth data (2014-2016) than the individual entity

## Name Variations Between Lists

A significant challenge identified is name variations between the two lists, particularly for Chinese billionaires:
- The same person appears as "Jack Ma" in Forbes but as "Ma Yun" in Hurun
  - This reflects the difference between Western naming convention (given name first) vs Chinese naming convention (family name first)
  - Our matching algorithm needs to account for these cultural naming differences
  - In this case, the wealth values also differ significantly (Hurun: $21-25 billion vs Forbes: $23.5-28.6 billion)
- This suggests we need to implement additional name matching rules specifically for handling these cultural naming variations

## Output Formats

### Basic ID Mapping

The `id_mappings.json` file follows the simplified format requested:
```json
{
  "unified_persons": [
    {
      "unified_id": "zong_qinghou",
      "hurun_ids": ["zong_qinghou", "zong_qinghou_family"],
      "forbes_ids": ["zong_qinghou"]
    }
  ]
}
```

### Expanded ID Mapping

The `id_mappings_expanded.json` file includes additional examples to demonstrate how we handle various matching challenges:

```json
{
  "unified_persons": [
    {
      "unified_id": "zong_qinghou",
      "hurun_ids": ["zong_qinghou", "zong_qinghou_family"],
      "forbes_ids": ["zong_qinghou"]
    },
    {
      "unified_id": "ma_yun",
      "hurun_ids": ["ma_yun"],
      "forbes_ids": ["jack_ma"]
    },
    {
      "unified_id": "elon_musk",
      "hurun_ids": ["elon_musk"],
      "forbes_ids": ["elon_musk"]
    }
  ]
}
```

These formats allow for straightforward integration into the final merging process, where additional data fields can be populated from the source datasets based on these ID mappings.
