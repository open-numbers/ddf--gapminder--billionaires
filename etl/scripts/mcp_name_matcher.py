#!/usr/bin/env python3
"""
MCP Server for embedding-based matching between Hurun and Forbes billionaire lists.
"""

import asyncio
import json
import pandas as pd
import sys
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
from sentence_transformers import SentenceTransformer
import pickle
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import fuzz
import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions


class EmbeddingMatcher:
    def __init__(self):
        self.model = SentenceTransformer("multi-qa-mpnet-base-dot-v1")
        self.hurun_data = None
        self.forbes_data = None
        self.embeddings = None
        self.metadata = None
        self.load_data()
        self.load_embeddings()

    def load_data(self):
        """Load Hurun and Forbes data from CSV files."""
        try:
            base_dir = Path(__file__).resolve().parent.parent
            # Load Hurun data
            hurun_path = base_dir / "intermediate/hurun/ddf--entities--person.csv"
            if hurun_path.exists():
                self.hurun_data = pd.read_csv(hurun_path).replace({np.nan: None})
                print(f"Loaded {len(self.hurun_data)} Hurun entries", file=sys.stderr)
            else:
                print(f"Hurun data not found at {hurun_path}", file=sys.stderr)

            # Load Forbes data
            forbes_path = base_dir / "intermediate/forbes/ddf--entities--person.csv"
            if forbes_path.exists():
                self.forbes_data = pd.read_csv(forbes_path).replace({np.nan: None})
                print(f"Loaded {len(self.forbes_data)} Forbes entries", file=sys.stderr)
            else:
                print(f"Forbes data not found at {forbes_path}", file=sys.stderr)

        except Exception as e:
            print(f"Error loading data: {e}", file=sys.stderr)

    def load_embeddings(self):
        """Load pre-generated embeddings and metadata."""
        try:
            base_dir = Path(__file__).resolve().parent.parent
            embeddings_path = (
                base_dir / "intermediate/embeddings/billionaire_embeddings.pkl"
            )
            if embeddings_path.exists():
                with open(embeddings_path, "rb") as f:
                    data = pickle.load(f)
                    self.embeddings = data["embeddings"]
                    self.metadata = data["metadata"]
                print(
                    f"Loaded {len(self.metadata)} billionaire embeddings",
                    file=sys.stderr,
                )
            else:
                print(f"Embeddings not found at {embeddings_path}", file=sys.stderr)
        except Exception as e:
            print(f"Error loading embeddings: {e}", file=sys.stderr)

    def get_wealth_data(self, person_id: str, source: str) -> Dict[str, Any]:
        """Get average wealth data for a person from the last 3 years."""
        wealth_data = {"average_wealth": None, "wealth_years": []}

        try:
            base_dir = Path(__file__).resolve().parent.parent
            if source == "hurun":
                wealth_path = (
                    base_dir
                    / "intermediate/hurun/ddf--datapoints--wealth--by--person--year.csv"
                )
            else:  # forbes
                wealth_path = (
                    base_dir
                    / "intermediate/forbes/ddf--datapoints--worth--by--person--year.csv"
                )

            if wealth_path.exists():
                wealth_df = pd.read_csv(wealth_path)
                person_wealth = wealth_df[wealth_df["person"] == person_id]

                if not person_wealth.empty:
                    # Get latest 3 years of data
                    person_wealth = person_wealth.sort_values(
                        "year", ascending=False
                    ).head(3)
                    wealth_data["wealth_years"] = person_wealth[
                        ["year", "worth" if source == "forbes" else "wealth"]
                    ].to_dict("records")
                    # Calculate average wealth
                    wealth_column = "worth" if source == "forbes" else "wealth"
                    wealth_data["average_wealth"] = person_wealth[wealth_column].mean()
        except Exception as e:
            print(f"Error loading wealth data: {e}", file=sys.stderr)

        return wealth_data

    def create_query_profile(
        self,
        name=None,
        country=None,
        company=None,
        birth_year=None,
        industry=None,
        gender=None,
    ):
        """Create a standardized query profile string matching the embedding generation format."""
        profile_parts = []

        # Always include all fields in the same order, using "n/a" for missing values

        # Name
        if name and pd.notna(name):
            profile_parts.append(f"Billionaire Name: {name}")
        else:
            profile_parts.append("Billionaire Name: n/a")

        # Country
        if country and pd.notna(country):
            profile_parts.append(f"Country: {country}")
        else:
            profile_parts.append("Country: n/a")

        # Company
        if company and pd.notna(company):
            profile_parts.append(f"Company: {company}")
        else:
            profile_parts.append("Company: n/a")

        # Birth Year
        if birth_year and pd.notna(birth_year):
            try:
                profile_parts.append(f"Birth Year: {int(birth_year)}")
            except (ValueError, TypeError):
                profile_parts.append("Birth Year: n/a")
        else:
            profile_parts.append("Birth Year: n/a")

        # Industry
        if industry and pd.notna(industry):
            profile_parts.append(f"Industry: {industry}")
        else:
            profile_parts.append("Industry: n/a")

        # Gender
        if gender and pd.notna(gender):
            profile_parts.append(f"Gender: {gender}")
        else:
            profile_parts.append("Gender: n/a")

        return " ".join(profile_parts)

    def embedding_search(
        self, person_id: str, source: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search for similar billionaires using embedding similarity.

        Args:
            person_id: The person ID to search for
            source: The source dataset ("hurun" or "forbes")
            limit: Maximum number of results to return

        Returns:
            List of matches with detailed information including demographics and wealth data
        """
        if self.embeddings is None or self.metadata is None:
            return []

        # Get the person's data from the appropriate dataset
        if source == "hurun" and self.hurun_data is not None:
            person_data = self.hurun_data[self.hurun_data["person"] == person_id]
        elif source == "forbes" and self.forbes_data is not None:
            person_data = self.forbes_data[self.forbes_data["person"] == person_id]
        else:
            return []

        if person_data.empty:
            return []

        person_row = person_data.iloc[0]

        # Create query profile
        query_profile = self.create_query_profile(
            name=person_row.get("name"),
            country=person_row.get("country"),
            company=person_row.get("company"),
            birth_year=person_row.get("birth_year"),
            industry=person_row.get("industry"),
            gender=person_row.get("gender"),
        )

        # Generate query embedding
        query_embedding = self.model.encode(query_profile)

        # Calculate cosine similarities
        similarities = cosine_similarity([query_embedding], self.embeddings)[0]

        # Get top matches
        top_indices = np.argsort(similarities)[-limit:][::-1]

        results = []
        for idx in top_indices:
            match = self.metadata[idx]
            match_person_id = match["person_id"]
            match_source = match["source"]

            # Get detailed person data
            if match_source == "hurun" and self.hurun_data is not None:
                person_row = self.hurun_data[
                    self.hurun_data["person"] == match_person_id
                ]
            elif match_source == "forbes" and self.forbes_data is not None:
                person_row = self.forbes_data[
                    self.forbes_data["person"] == match_person_id
                ]
            else:
                continue

            if person_row.empty:
                continue

            person_row = person_row.iloc[0]

            # Get wealth data
            wealth_data = self.get_wealth_data(match_person_id, match_source)

            results.append(
                {
                    "source": match_source,
                    "name": person_row.get("name", ""),
                    "chinese_name": person_row.get("chinese_name", None),
                    "person_id": match_person_id,
                    "similarity_score": float(similarities[idx]),
                    "birth_year": person_row.get("birth_year", None),
                    "gender": person_row.get("gender", None),
                    "country": person_row.get("country", None),
                    "industry": person_row.get("industry", None),
                    "company": person_row.get("company", None),
                    "headquarter": person_row.get("headquarter", None),
                    "title": person_row.get("title", None),
                    "average_wealth": wealth_data["average_wealth"],
                    "wealth_history": wealth_data["wealth_years"],
                }
            )

        return results

    def fuzzy_name_search(
        self, name_query: str, source: str = None, limit: int = 10, min_score: int = 70
    ) -> List[Dict[str, Any]]:
        """
        Search for billionaires by fuzzy matching their names.

        Args:
            name_query: The name to search for
            source: Optional source filter ("hurun", "forbes", or None for both)
            limit: Maximum number of results to return
            min_score: Minimum fuzzy match score (0-100)

        Returns:
            List of matches with detailed information
        """
        all_candidates = []

        # Collect candidates from Hurun
        if source in [None, "hurun"] and self.hurun_data is not None:
            for _, row in self.hurun_data.iterrows():
                name = row.get("name", "")
                chinese_name = row.get("chinese_name", "")
                if pd.notna(name):
                    all_candidates.append(
                        {
                            "name": name,
                            "chinese_name": chinese_name
                            if pd.notna(chinese_name)
                            else None,
                            "person_id": row.get("person", ""),
                            "source": "hurun",
                            "row": row,
                        }
                    )

        # Collect candidates from Forbes
        if source in [None, "forbes"] and self.forbes_data is not None:
            for _, row in self.forbes_data.iterrows():
                name = row.get("name", "")
                if pd.notna(name):
                    all_candidates.append(
                        {
                            "name": name,
                            "chinese_name": None,
                            "person_id": row.get("person", ""),
                            "source": "forbes",
                            "row": row,
                        }
                    )

        # Perform fuzzy matching
        matches = []
        for candidate in all_candidates:
            # Match against English name
            name_score = fuzz.WRatio(name_query.lower(), candidate["name"].lower())

            # Also match against Chinese name if available
            chinese_score = 0
            if candidate["chinese_name"]:
                chinese_score = fuzz.ratio(name_query, candidate["chinese_name"])

            # Use the higher score
            final_score = max(name_score, chinese_score)

            if final_score >= min_score:
                matches.append({"candidate": candidate, "score": final_score})

        # Sort by score (descending) and limit results
        matches.sort(key=lambda x: x["score"], reverse=True)
        matches = matches[:limit]

        # Format results
        results = []
        for match in matches:
            candidate = match["candidate"]
            row = candidate["row"]
            person_id = candidate["person_id"]
            source = candidate["source"]

            # Get wealth data
            wealth_data = self.get_wealth_data(person_id, source)

            results.append(
                {
                    "source": source,
                    "name": row.get("name", ""),
                    "chinese_name": row.get("chinese_name", None),
                    "person_id": person_id,
                    "fuzzy_score": match["score"],
                    "birth_year": row.get("birth_year", None),
                    "gender": row.get("gender", None),
                    "country": row.get("country", None),
                    "industry": row.get("industry", None),
                    "company": row.get("company", None),
                    "headquarter": row.get("headquarter", None),
                    "title": row.get("title", None),
                    "average_wealth": wealth_data["average_wealth"],
                    "wealth_history": wealth_data["wealth_years"],
                }
            )

        return results


# Initialize the embedding matcher
embedding_matcher = EmbeddingMatcher()

# Create the MCP server
server = Server("embedding-matcher")


@server.list_tools()
async def handle_list_tools() -> List[types.Tool]:
    """List available tools."""
    return [
        types.Tool(
            name="embedding_search",
            description="Search for similar billionaires using embedding similarity based on a person ID from either Hurun or Forbes datasets. Returns detailed information including demographics, company info, and latest wealth data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "person_id": {
                        "type": "string",
                        "description": "The person ID to search for. (must be lowercase alphanumeric, connect with underscore)",
                    },
                    "list": {
                        "type": "string",
                        "description": "The source dataset: either 'hurun' or 'forbes'",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Total number of results to return (default: 10)",
                        "default": 10,
                    },
                },
                "required": ["person_id", "list"],
            },
        ),
        types.Tool(
            name="fuzzy_name_search",
            description="Search for billionaires by name using fuzzy string matching. Can search in English names and Chinese names (for Hurun data). Returns matches sorted by similarity score.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The name to search for (can be partial or slightly misspelled)",
                    },
                    "list": {
                        "type": "string",
                        "description": "Optional source filter: 'hurun', 'forbes', or omit for both",
                        "enum": ["hurun", "forbes"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default: 10)",
                        "default": 10,
                    },
                    "min_score": {
                        "type": "integer",
                        "description": "Minimum fuzzy match score 0-100 (default: 70)",
                        "default": 70,
                        "minimum": 0,
                        "maximum": 100,
                    },
                },
                "required": ["name"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> List[types.TextContent]:
    """Handle tool calls."""
    if name == "embedding_search":
        person_id = arguments.get("person_id", "")
        source = arguments.get("list", "")
        limit = arguments.get("limit", 10)

        if not person_id:
            return [
                types.TextContent(
                    type="text", text="Error: person_id parameter is required"
                )
            ]

        if source not in ["hurun", "forbes"]:
            return [
                types.TextContent(
                    type="text",
                    text="Error: list parameter must be either 'hurun' or 'forbes'",
                )
            ]

        try:
            results = embedding_matcher.embedding_search(person_id, source, limit)

            # Format results as JSON
            response = {
                "query_person_id": person_id,
                "query_source": source,
                "matches": results,
                "total_matches": len(results),
            }

            return [types.TextContent(type="text", text=json.dumps(response, indent=2))]

        except Exception as e:
            return [
                types.TextContent(
                    type="text", text=f"Error performing search: {str(e)}"
                )
            ]

    elif name == "fuzzy_name_search":
        name_query = arguments.get("name", "")
        source = arguments.get("list", None)
        limit = arguments.get("limit", 10)
        min_score = arguments.get("min_score", 70)

        if not name_query:
            return [
                types.TextContent(type="text", text="Error: name parameter is required")
            ]

        if source and source not in ["hurun", "forbes"]:
            return [
                types.TextContent(
                    type="text",
                    text="Error: list parameter must be either 'hurun', 'forbes', or omitted",
                )
            ]

        try:
            results = embedding_matcher.fuzzy_name_search(
                name_query, source, limit, min_score
            )

            # Format results as JSON
            response = {
                "query_name": name_query,
                "source_filter": source,
                "min_score": min_score,
                "matches": results,
                "total_matches": len(results),
            }

            return [types.TextContent(type="text", text=json.dumps(response, indent=2))]

        except Exception as e:
            return [
                types.TextContent(
                    type="text", text=f"Error performing fuzzy search: {str(e)}"
                )
            ]

    else:
        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    """Run the MCP server."""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="embedding-matcher",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
