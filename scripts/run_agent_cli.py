#!/usr/bin/env python3
"""Programmatic Google ADK 2.x CLI Runner (`run_agent_cli.py`).

Streams thoughts, sub-agent transfers (`transfer_to_agent`), tool invocations
(`discover_federated_tables`, `execute_sql`, `inspect_bigquery_job_cache_stats`),
and synthesized responses right in your terminal across tiered Gemini models
(`gemini-4-argon`, `gemini-4-carbon`, and `gemini-3.8-flash`).

Usage:
  # Inspect the wired multi-agent graph & per-role Gemini models offline:
  uv run python scripts/run_agent_cli.py --inspect-topology

  # Switch model profile on the fly (tiered | argon | carbon | flash):
  uv run python scripts/run_agent_cli.py --inspect-topology --model-profile carbon

  # Run a live end-to-end prompt against your Lakehouse:
  uv run python scripts/run_agent_cli.py --prompt "Which US-NORTHEAST categories have the highest margin exposure, and what was our cross-cloud cache hit ratio?"
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from google.adk.runners import InMemoryRunner
from google.genai import types

from lakehouse_agent.agent import BIGQUERY_ALLOWED_TOOLS, build_lakehouse_agent_system
from lakehouse_agent.config import MODEL_PROFILES, load_settings


def print_agent_topology(model_profile: str | None = None) -> None:
    cfg = load_settings(model_profile_override=model_profile)
    root = build_lakehouse_agent_system(cfg)
    root_think = root.planner.thinking_config.thinking_level.name
    max_tok = root.generate_content_config.max_output_tokens
    print("=" * 88)
    print(f" Google ADK 2.x Multi-Agent Topology (profile='{cfg.model_profile}', max_output_tokens={max_tok})")
    print("=" * 88)
    print(f" Root Orchestrator : {root.name} (model={root.model}, thinking={root_think})")
    for sub in root.sub_agents:
        sub_think = sub.planner.thinking_config.thinking_level.name
        print(f"   ├── Sub-Agent   : {sub.name} (model={sub.model}, thinking={sub_think})")
        for tool in sub.tools:
            tname = getattr(tool, "__name__", tool.__class__.__name__)
            if tname == "BigQueryToolset":
                print(f"   │     └── Tool  : BigQueryToolset (filter={BIGQUERY_ALLOWED_TOOLS})")
            else:
                print(f"   │     └── Tool  : {tname}")
    print("=" * 88)


async def run_live_prompt(
    prompt: str,
    user_token: str | None = None,
    model_profile: str | None = None,
) -> None:
    cfg = load_settings(model_profile_override=model_profile)
    agent = build_lakehouse_agent_system(cfg, user_access_token=user_token)
    runner = InMemoryRunner(agent=agent, app_name="borderless-lakehouse-poc")
    session = await runner.session_service.create_session(
        app_name="borderless-lakehouse-poc",
        user_id="cloud-practitioner",
    )

    content = types.Content(role="user", parts=[types.Part.from_text(text=prompt)])
    print(f"\nUser > {prompt}\n" + "-" * 88)

    async for event in runner.run_async(
        user_id="cloud-practitioner",
        session_id=session.id,
        new_message=content,
    ):
        author = getattr(event, "author", "agent")
        ev_content = getattr(event, "content", None)
        if not ev_content or not ev_content.parts:
            continue
        for part in ev_content.parts:
            if getattr(part, "thought", False) and part.text:
                snippet = part.text.strip().replace("\n", " ")[:140]
                print(f"  [{author} thought] {snippet}...")
            elif part.function_call:
                fc = part.function_call
                print(f"  [{author}] -> {fc.name}({dict(fc.args or {})})")
            elif part.function_response:
                fr = part.function_response
                resp_preview = str(fr.response)[:160]
                print(f"    ↳ [{fr.name}] {resp_preview}...")
            elif part.text:
                print(f"\n[{author}] >\n{part.text.strip()}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Borderless Lakehouse ADK CLI Runner")
    parser.add_argument(
        "--inspect-topology",
        action="store_true",
        help="Print the wired multi-agent graph, per-role Gemini models, and tool guardrails offline",
    )
    parser.add_argument(
        "--model-profile",
        choices=sorted(MODEL_PROFILES.keys()),
        default=None,
        help="Override model routing profile (tiered | argon | carbon | flash)",
    )
    parser.add_argument(
        "--prompt",
        default=(
            "Which US-NORTHEAST product categories have the highest gross margin exposure "
            "over the past 7 days, and what was the latest BigQuery job's cache hit ratio?"
        ),
        help="User prompt to execute through lakehouse_practitioner_copilot",
    )
    parser.add_argument(
        "--user-access-token",
        default=None,
        help="Optional end-user OAuth access token for Databricks Unity Catalog identity propagation",
    )
    args = parser.parse_args()

    if args.inspect_topology:
        print_agent_topology(model_profile=args.model_profile)
        return 0

    asyncio.run(
        run_live_prompt(
            args.prompt,
            user_token=args.user_access_token,
            model_profile=args.model_profile,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
