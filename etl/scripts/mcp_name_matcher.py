#!/usr/bin/env python3
"""
MCP Server for fuzzy name matching between Hurun and Forbes billionaire lists.
"""

import asyncio
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
from fuzzywuzzy import fuzz, process
import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions


class NameMatcher:
    def __init__(self):
        self.hurun_data = None
        self.forbes_data = None
        self.load_data()

    def load_data(self):
        """Load Hurun and Forbes data from CSV files."""
        try:
            # Load Hurun data
            hurun_path = Path("../intermediate/hurun/ddf--entities--person.csv")
            if hurun_path.exists():
                self.hurun_data = pd.read_csv(hurun_path)
                print(f"Loaded {len(self.hurun_data)} Hurun entries")
            else:
                print(f"Hurun data not found at {hurun_path}")

            # Load Forbes data
            forbes_path = Path("../intermediate/forbes/ddf--entities--person.csv")
            if forbes_path.exists():
                self.forbes_data = pd.read_csv(forbes_path)
                print(f"Loaded {len(self.forbes_data)} Forbes entries")
            else:
                print(f"Forbes data not found at {forbes_path}")

        except Exception as e:
            print(f"Error loading data: {e}")

    def get_wealth_data(self, person_id: str, source: str) -> Dict[str, Any]:
        """Get average wealth data for a person from the last 3 years."""
        wealth_data = {"average_wealth": None, "wealth_years": []}

        try:
            if source == "hurun":
                wealth_path = Path(
                    "../intermediate/hurun/ddf--datapoints--wealth--by--person--year.csv"
                )
            else:  # forbes
                wealth_path = Path(
                    "../intermediate/forbes/ddf--datapoints--worth--by--person--year.csv"
                )

            if wealth_path.exists():
                wealth_df = pd.read_csv(wealth_path)
                person_wealth = wealth_df[wealth_df["person"] == person_id]

                if not person_wealth.empty:
                    # Get latest 3 years of data
                    person_wealth = person_wealth.sort_values("year", ascending=False).head(3)
                    wealth_data["wealth_years"] = person_wealth[
                        ["year", "worth" if source == "forbes" else "wealth"]
                    ].to_dict("records")
                    # Calculate average wealth
                    wealth_column = "worth" if source == "forbes" else "wealth"
                    wealth_data["average_wealth"] = person_wealth[wealth_column].mean()
        except Exception as e:
            print(f"Error loading wealth data: {e}")

        return wealth_data

    def fuzzy_search_name(self, query_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search for similar names in both Hurun and Forbes datasets.

        Args:
            query_name: Name to search for
            limit: Maximum number of results to return per dataset

        Returns:
            List of matches with detailed information including demographics and wealth data
        """
        results = []

        # Search in Hurun data
        if self.hurun_data is not None and "name" in self.hurun_data.columns:
            hurun_names = self.hurun_data["name"].dropna().tolist()
            hurun_matches = process.extract(query_name, hurun_names, limit=limit, scorer=fuzz.ratio)

            for match_name, score in hurun_matches:
                # Find the person_id for this name
                person_row = self.hurun_data[self.hurun_data["name"] == match_name].iloc[0]
                person_id = person_row.get("person", "")

                # Get wealth data
                wealth_data = self.get_wealth_data(person_id, "hurun")

                results.append(
                    {
                        "source": "hurun",
                        "name": match_name,
                        "person_id": person_id,
                        "similarity_score": score,
                        "birth_year": person_row.get("birth_year", None),
                        "gender": person_row.get("gender", None),
                        "country": person_row.get("country", None),
                        "industry": person_row.get("industry", None),
                        "company": person_row.get("company", None),
                        "title": person_row.get("title", None),
                        "average_wealth": wealth_data["average_wealth"],
                        "wealth_history": wealth_data["wealth_years"],
                    }
                )

        # Search in Forbes data
        if self.forbes_data is not None and "name" in self.forbes_data.columns:
            forbes_names = self.forbes_data["name"].dropna().tolist()
            forbes_matches = process.extract(
                query_name, forbes_names, limit=limit, scorer=fuzz.ratio
            )

            for match_name, score in forbes_matches:
                # Find the person_id for this name
                person_row = self.forbes_data[self.forbes_data["name"] == match_name].iloc[0]
                person_id = person_row.get("person", "")

                # Get wealth data
                wealth_data = self.get_wealth_data(person_id, "forbes")

                results.append(
                    {
                        "source": "forbes",
                        "name": match_name,
                        "person_id": person_id,
                        "similarity_score": score,
                        "birth_year": person_row.get("birth_year", None),
                        "gender": person_row.get("gender", None),
                        "country": person_row.get("country", None),
                        "industry": person_row.get("industry", None),
                        "company": person_row.get("company", None),
                        "title": person_row.get("title", None),
                        "average_wealth": wealth_data["average_wealth"],
                        "wealth_history": wealth_data["wealth_years"],
                    }
                )

        # Sort by similarity score descending
        results.sort(key=lambda x: x["similarity_score"], reverse=True)

        return results


# Initialize the name matcher
name_matcher = NameMatcher()

# Create the MCP server
server = Server("name-matcher")


@server.list_tools()
async def handle_list_tools() -> List[types.Tool]:
    """List available tools."""
    return [
        types.Tool(
            name="fuzzy_name_search",
            description="Search for similar names in both Hurun and Forbes billionaire datasets using fuzzy matching. Returns detailed information including demographics, company info, and latest wealth data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "The name to search for"},
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return per dataset (default: 5)",
                        "default": 5,
                    },
                },
                "required": ["name"],
            },
        )
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> List[types.TextContent]:
    """Handle tool calls."""
    if name == "fuzzy_name_search":
        query_name = arguments.get("name", "")
        limit = arguments.get("limit", 5)

        if not query_name:
            return [types.TextContent(type="text", text="Error: Name parameter is required")]

        try:
            results = name_matcher.fuzzy_search_name(query_name, limit)

            # Format results as JSON
            response = {"query": query_name, "matches": results, "total_matches": len(results)}

            return [types.TextContent(type="text", text=json.dumps(response, indent=2))]

        except Exception as e:
            return [types.TextContent(type="text", text=f"Error performing search: {str(e)}")]

    else:
        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    """Run the MCP server."""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="name-matcher",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
