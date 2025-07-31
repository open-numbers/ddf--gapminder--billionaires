# Methodology for Collecting and Merging Billionaire Data

## Introduction

The goal of this project is to compile a comprehensive dataset of billionaires by combining data from two major sources: the Forbes Billionaires List (published by Forbes in the United States) and the Hurun Rich List (published by Hurun in China). By merging these lists, we aim to create a more inclusive and global view of billionaire data. Both lists are reported annually, so we will focus on historical data across years. Key challenges include inconsistent naming, data discrepancies (e.g., differing citizenship attributions for individuals with multiple nationalities), and ensuring uniqueness. We will leverage large language models (LLMs) to assist with summarization and merging.

## Step 1: Data Collection

- Gather historical billionaire lists from Forbes and Hurun.
- Collect data year by year to capture changes over time (e.g., net worth fluctuations, new entrants).
- Sources may include official websites, APIs, or archived datasets—ensure data is scraped or downloaded ethically and legally.

## Step 2: Data Cleaning

- **Handle Inconsistencies in Each List:** Names may vary due to transliterations, abbreviations, or formatting (e.g., "Elon Musk" vs. "Musk, Elon"). Standardize names, ages, net worth, and other attributes.
- **Assign Unique IDs:** For each list independently, create a unique identifier for every billionaire. This could be based on a combination of name, birthdate, country, and other stable attributes to avoid duplicates within the same list.
- Remove any invalid or incomplete entries.
### Hurun-Specific Cleaning Issues

- **Handling Duplicate-Like Entries**: During the processing of the Hurun list, it was noted that some entries might appear to be duplicates but represent different entities (e.g., `zong_qinghou_family` and `zong_qinghou`). For now, these will be treated as distinct entities. The final decision on how to handle such cases will be made during the merging phase with the Forbes list.


- **No Unique IDs:** The Hurun lists do not provide inherent unique identifiers for individuals. We will generate these programmatically, similar to the general approach, using a hash or combination of stable attributes like full name, birth year, and other available details.

- **Format Differences Across Years:** 
  - Data from 2019-2025 follows a consistent format.
  - Pre-2019 data uses a different format and often lacks country information for individuals. To address this, we will impute missing country data where possible by cross-referencing with later Hurun entries, Forbes data, or using LLMs to infer based on other attributes (e.g., name origins or known residences). If imputation is unreliable, flag these entries for manual review.

### Forbes-Specific Cleaning Issues

- **Unique IDs via URIs:** Forbes provides a URI for each person's page on their website, which we assume remains stable over time. Use this URI as the unique identifier for individuals in the Forbes list.

- **Junk Data Removal:** The list contains some garbage data identifiable by net worth values that are floating-point numbers. Remove any entries where the net worth is a floating-point number.

## Step 3: Merging the Lists

### Phase 1: Building Fuzzy Query Tools

- **MCP Server for Name Matching:** Build an MCP (Model Context Protocol) server that provides fuzzy name matching capabilities for both Hurun and Forbes datasets. This server will:
  - Accept search queries with person names
  - Return ranked results from both Hurun and Forbes lists using fuzzy matching algorithms
  - Provide standardized query interfaces that can be used by LLMs
  - Handle variations in name formatting, transliterations, and partial matches

### Phase 2: LLM-Assisted Matching Process

- **Forbes-Based Matching Pipeline:** Using Forbes as the base dataset, develop a tool that uses LLMs to systematically process names:
  1. For each person in the Hurun list, query the MCP server to find potential matches in both Hurun and Forbes data
  2. Present query results to the LLM with context about the individuals
  3. Let the LLM decide which query results represent the same person based on name similarity
  4. Append all Forbes IDs that didn't get matched during this process

- **Output Format:** Generate a JSON file containing:
  - Unified person IDs (following the slug-based naming convention described earlier)
  - Mapped Hurun person IDs for each unified person (where matches exist)
  - Mapped Forbes person IDs for each unified person
  - All unmatched Forbes entries will be included as separate unified persons

### Phase 3: Manual Review and Validation

- **Identify Matches:** The automated process will handle the bulk of matching, with manual review for edge cases flagged by the LLM
- **Resolve Discrepancies:** 
  - For conflicting information (e.g., different citizenships, net worth values, or industries), prioritize based on source reliability or use averages/rules (e.g., list multiple citizenships if applicable)
  - Handle cases where one list includes individuals not present in the other
- **Create a Unified List:** Combine into a single dataset with unique global IDs, preserving source-specific data where relevant

### Defining a Unified Unique ID

To ensure every entity in the final merged dataset has a stable and unique identifier, we will adopt the following strategy:

1.  **Base ID Generation**: A "slug" is created from the person's or entity's name. This process involves:
    -   Converting the name to lowercase.
    -   Replacing spaces and special characters with underscores (`_`).
    -   Examples: "Elon Musk" becomes `elon_musk`; "Wessels Family" becomes `wessels_family`.

2.  **Uniqueness Enforcement**:
    -   The first time a name is encountered, its unique ID is the generated slug (e.g., `john_smith`).
    -   If another entity with the same name is found, a numeric suffix is appended to ensure uniqueness. The second instance will be `john_smith_2`, the third `john_smith_3`, and so on.

This approach is simple, robust, and accommodates both individual billionaires and group entities without relying on potentially missing or inconsistent data like birth years or citizenship.

## Role of Large Language Models

- Employ LLMs to:
  - Summarize and analyze individual lists for patterns or anomalies.
  - Assist in automated merging by generating match probabilities, suggesting resolutions for conflicts, or flagging potential duplicates.
- This will enhance efficiency, especially for large-scale historical data.

## Potential Challenges and Mitigations

- **Data Privacy and Ethics:** Ensure compliance with data usage policies from Forbes and Hurun.
- **Accuracy:** Validate merged data against additional sources if possible.
- **Scalability:** Process data in batches for efficiency.

This methodology will evolve as we implement and test the process.
