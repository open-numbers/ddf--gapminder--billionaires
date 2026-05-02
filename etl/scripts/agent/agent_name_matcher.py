"""
An agent for working with billionaire data built with openai-agents.
"""

import asyncio
import json
import pandas as pd
import random
import os
from agents import (
    Agent,
    RunContextWrapper,
    Runner,
    function_tool,
    set_default_openai_api,
    set_default_openai_client,
    set_tracing_disabled,
)
from agents.exceptions import MaxTurnsExceeded
from agents.mcp import MCPServerStdio
from openai import AsyncOpenAI
from typing import List, Tuple, Dict
from dataclasses import dataclass


set_tracing_disabled(disabled=True)


def load_agent_instructions():
    """Load agent instructions from the prompt file."""
    prompt_path = os.path.join(os.path.dirname(__file__), "agent_prompt.txt")
    try:
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        print(f"Warning: Could not find {prompt_path}")
        raise


# define local context object
# 1. the current mapping
# 2. the reverse mapping - which will be useful in checking if a id is mapped.
@dataclass
class IDMappingItem:
    unified_person_id: str
    hurun_ids: List[str]
    forbes_ids: List[str]


@dataclass
class IDMapping:
    mapping: List[IDMappingItem]
    reverse_mapping: Dict[Tuple[str, str], str]


# function call to get current mapping
@function_tool
def get_mappings(
    wrapper: RunContextWrapper[IDMapping], key: str | None = None
) -> List[IDMappingItem]:
    """if key (a unified person ID) is provided, only return the mapping for the key. Otherwise return the entire list"""
    if key:
        # Filter to only return the mapping for the specified unified_person_id
        return [
            item for item in wrapper.context.mapping if item.unified_person_id == key
        ]
    return wrapper.context.mapping


# function call to insert mapping to list
@function_tool
def insert_mapping(
    wrapper: RunContextWrapper[IDMapping],
    unified_person_id: str,
    hurun_id: str | None = None,
    forbes_id: str | None = None,
) -> str:
    """insert a mapping and return a message about if record inserted successfully. Also update the reverse mapping."""
    # Check if hurun_id is already mapped to a different unified_person_id
    if hurun_id:
        existing_hurun_mapping = wrapper.context.reverse_mapping.get(
            ("hurun", hurun_id)
        )
        if existing_hurun_mapping and existing_hurun_mapping != unified_person_id:
            return f"Error: hurun_id {hurun_id} is already mapped to {existing_hurun_mapping}, cannot map to {unified_person_id}"

    # Check if forbes_id is already mapped to a different unified_person_id
    if forbes_id:
        existing_forbes_mapping = wrapper.context.reverse_mapping.get(
            ("forbes", forbes_id)
        )
        if existing_forbes_mapping and existing_forbes_mapping != unified_person_id:
            return f"Error: forbes_id {forbes_id} is already mapped to {existing_forbes_mapping}, cannot map to {unified_person_id}"

    # Check if this unified_id already exists in mapping
    existing_item = None
    for item in wrapper.context.mapping:
        if item.unified_person_id == unified_person_id:
            existing_item = item
            break

    if existing_item:
        # Update existing mapping
        if hurun_id and hurun_id not in existing_item.hurun_ids:
            existing_item.hurun_ids.append(hurun_id)
            wrapper.context.reverse_mapping[("hurun", hurun_id)] = unified_person_id

        if forbes_id and forbes_id not in existing_item.forbes_ids:
            existing_item.forbes_ids.append(forbes_id)
            wrapper.context.reverse_mapping[("forbes", forbes_id)] = unified_person_id

        return f"Updated existing mapping for {unified_person_id}"
    else:
        # Create new mapping
        hurun_ids = [hurun_id] if hurun_id else []
        forbes_ids = [forbes_id] if forbes_id else []

        new_item = IDMappingItem(
            unified_person_id=unified_person_id,
            hurun_ids=hurun_ids,
            forbes_ids=forbes_ids,
        )

        wrapper.context.mapping.append(new_item)

        # Update reverse mapping
        if hurun_id:
            wrapper.context.reverse_mapping[("hurun", hurun_id)] = unified_person_id
        if forbes_id:
            wrapper.context.reverse_mapping[("forbes", forbes_id)] = unified_person_id

        return f"Created new mapping for {unified_person_id}"


@function_tool
def delete_mapping(
    wrapper: RunContextWrapper[IDMapping], unified_person_id: str
) -> str:
    """delete a mapping by unified_person_id and remove all associated reverse mappings."""
    # Find the mapping item to delete
    item_to_delete = None
    for item in wrapper.context.mapping:
        if item.unified_person_id == unified_person_id:
            item_to_delete = item
            break

    if not item_to_delete:
        return f"Error: unified_person_id {unified_person_id} not found in mapping"

    # Remove all reverse mappings associated with this unified_person_id
    # Remove hurun reverse mappings
    for hurun_id in item_to_delete.hurun_ids:
        key = ("hurun", hurun_id)
        if key in wrapper.context.reverse_mapping:
            del wrapper.context.reverse_mapping[key]

    # Remove forbes reverse mappings
    for forbes_id in item_to_delete.forbes_ids:
        key = ("forbes", forbes_id)
        if key in wrapper.context.reverse_mapping:
            del wrapper.context.reverse_mapping[key]

    # Remove the item from the main mapping list
    wrapper.context.mapping.remove(item_to_delete)

    return f"Successfully deleted mapping for unified_person_id {unified_person_id} and all associated reverse mappings"


def load_mapping_from_json(filename: str = "mapping.json") -> IDMapping:
    """Load an existing mapping from a JSON file."""
    if not os.path.exists(filename):
        print(f"No existing mapping found at {filename}, starting fresh.")
        return IDMapping(mapping=[], reverse_mapping={})

    with open(filename, "r", encoding="utf-8") as f:
        mapping_data = json.load(f)

    mapping_list = [
        IDMappingItem(
            unified_person_id=item["unified_person_id"],
            hurun_ids=item["hurun_ids"],
            forbes_ids=item["forbes_ids"],
        )
        for item in mapping_data
    ]

    # Rebuild reverse mapping
    reverse = {}
    for item in mapping_list:
        for hid in item.hurun_ids:
            reverse[("hurun", hid)] = item.unified_person_id
        for fid in item.forbes_ids:
            reverse[("forbes", fid)] = item.unified_person_id

    print(
        f"Loaded {len(mapping_list)} mappings from {filename} "
        f"({sum(1 for k in reverse if k[0] == 'hurun')} hurun, "
        f"{sum(1 for k in reverse if k[0] == 'forbes')} forbes IDs)"
    )
    return IDMapping(mapping=mapping_list, reverse_mapping=reverse)


def save_mapping_to_json(mapping: IDMapping, filename: str = "mapping.json"):
    """Save the mapping to a JSON file."""
    # Convert dataclasses to dictionaries for JSON serialization
    mapping_data = [
        {
            "unified_person_id": item.unified_person_id,
            "hurun_ids": item.hurun_ids,
            "forbes_ids": item.forbes_ids,
        }
        for item in mapping.mapping
    ]

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(mapping_data, f, indent=2, ensure_ascii=False)

    print(f"Mapping saved to {filename}")


async def main():
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    venv_python = os.path.join(project_root, ".venv", "bin", "python")
    mcp_server_script = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "mcp_name_matcher.py")
    )

    async with MCPServerStdio(
        params={
            "command": venv_python,
            "args": [mcp_server_script],
        },
        client_session_timeout_seconds=60,
    ) as billionaire_server:
        mapping = load_mapping_from_json(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "intermediate",
                "mapping.json",
            )
        )
        # Load agent instructions from file
        instructions = load_agent_instructions()

        print("Using DeepSeek API (deepseek-v4-pro)")
        # Use DeepSeek API (OpenAI-compatible)
        deepseek_client = AsyncOpenAI(
            base_url="https://api.deepseek.com",
            api_key=os.environ.get("DEEPSEEK_API_KEY"),
        )
        set_default_openai_client(deepseek_client)
        # Use chat/completions endpoint (not Responses API)
        set_default_openai_api("chat_completions")

        agent = Agent[IDMapping](
            name="billionaire-mapping-agent",
            instructions=instructions,
            mcp_servers=[billionaire_server],
            tools=[get_mappings, insert_mapping, delete_mapping],
            model="deepseek-v4-pro",
        )

        # Load both Hurun and Forbes person data
        forbes_csv_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "intermediate",
            "forbes",
            "ddf--entities--person.csv",
        )
        hurun_csv_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "intermediate",
            "hurun",
            "ddf--entities--person.csv",
        )

        forbes_df = pd.read_csv(forbes_csv_path)
        hurun_df = pd.read_csv(hurun_csv_path)

        # Create tracking list with all entities from both lists
        entities_to_check = []

        # Add Forbes entities
        for _, row in forbes_df.iterrows():
            entities_to_check.append(("forbes", row["person"]))

        # Add Hurun entities
        for _, row in hurun_df.iterrows():
            entities_to_check.append(("hurun", row["person"]))

        print(f"Initial entities to check: {len(entities_to_check)} total")
        print(f"  - Forbes: {len(forbes_df)} entities")
        print(f"  - Hurun: {len(hurun_df)} entities")

        # randomly choose 10 entities to check
        # entities_to_check = random.sample(entities_to_check, 10)
        # shuffle the list
        random.shuffle(entities_to_check)

        # Process until tracking list is empty
        iteration = 0
        while entities_to_check:
            iteration += 1
            print(f"\n=== Iteration {iteration} ===")
            # print(f"Entities remaining: {len(entities_to_check)}")

            # Get next entity to check
            list_type, person_id = entities_to_check.pop(0)
            print(f"Processing {list_type} entity: {person_id}")

            # Check if already mapped (reverse_mapping key is (list_type, person_id))
            existing_mapping = mapping.reverse_mapping.get((list_type, person_id))
            if existing_mapping:
                print(f"  Already mapped to {existing_mapping}, skipping")
                continue

            # Query agent to create mapping (this is an incremental update run)
            query = (
                f"This is an incremental update with new data. "
                f"Search for {person_id} from the {list_type} list. "
                f"This person may already have a mapping under a different ID "
                f"(IDs can change between data releases), or may be entirely new. "
                f"Use get_mappings() and search tools to determine the correct "
                f"unified ID, then insert the mapping."
            )
            try:
                result = await Runner.run(agent, query, context=mapping, max_turns=20)
            except MaxTurnsExceeded:
                print(f"Max turns exceeded: {person_id}, {list_type}")
                print("Manual Review required")
                continue

            print(f"Result: {result.final_output}")

            # Remove mapped entities from tracking list
            # Get current mappings to see what was just mapped
            current_mappings = mapping.mapping

            # Create a set of all mapped IDs for quick lookup
            mapped_forbes_ids = set()
            mapped_hurun_ids = set()

            for mapping_item in current_mappings:
                mapped_forbes_ids.update(mapping_item.forbes_ids)
                mapped_hurun_ids.update(mapping_item.hurun_ids)

            # Remove mapped entities from tracking list
            entities_to_check = [
                (list_type, entity_id)
                for list_type, entity_id in entities_to_check
                if (list_type == "forbes" and entity_id not in mapped_forbes_ids)
                or (list_type == "hurun" and entity_id not in mapped_hurun_ids)
            ]

            print(f"  Entities remaining after cleanup: {len(entities_to_check)}")

            # Save progress periodically
            if iteration % 50 == 0:
                save_mapping_to_json(
                    mapping,
                    os.path.join(
                        os.path.dirname(__file__),
                        "..",
                        "..",
                        "intermediate",
                        f"mapping_progress_iter_{iteration}.json",
                    ),
                )

        print("\n=== Processing Complete ===")
        print(f"Total iterations: {iteration}")
        print(f"Final mapping count: {len(mapping.mapping)}")

        # Save final mapping to JSON file
        mapping_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "intermediate", "mapping.json"
        )
        save_mapping_to_json(mapping, mapping_path)


if __name__ == "__main__":
    asyncio.run(main())
