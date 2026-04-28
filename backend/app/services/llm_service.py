from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from langchain_core.prompts import ChatPromptTemplate

try:
    from langchain_groq import ChatGroq
except ImportError:  # pragma: no cover
    ChatGroq = None

from app.core.config import get_settings


class LLMService:
    def __init__(self) -> None:
        self._settings = get_settings()
        prompts_dir = Path(__file__).resolve().parents[1] / "prompts"
        self._request_planner_path = prompts_dir / "request_planner.txt"
        self._sql_generator_path = prompts_dir / "sql_generator.txt"
        self._result_analyst_path = prompts_dir / "result_analyst.txt"

    def plan_request(self, user_question: str, asset_catalog_context: str) -> dict[str, Any]:
        fallback = {
            "asset_id": "",
            "analysis_name": "live_status",
            "time_window": {
                "scope": "historical",
                "value": 1,
                "unit": "hours",
                "label": "last 1 hour",
            },
            "reasoning": "The planner could not run, so no asset was identified.",
            "prompt_text": "",
        }
        system_prompt = self._request_planner_path.read_text(encoding="utf-8")
        human_prompt = (
            "User request:\n{user_question}\n\n"
            "Asset catalog:\n{asset_catalog_context}\n"
        )
        variables = {
            "user_question": user_question,
            "asset_catalog_context": self._clip_text(asset_catalog_context, 2200),
        }
        result = self._invoke_json(
            system_prompt=system_prompt,
            human_prompt=human_prompt,
            variables=variables,
            fallback=fallback,
        )
        result["prompt_text"] = self._render_prompt_text(system_prompt, human_prompt, variables)
        return result

    def generate_sql(
        self,
        user_question: str,
        asset_context: str,
        analysis_name: str,
        time_window_label: str,
    ) -> dict[str, Any]:
        system_prompt = self._sql_generator_path.read_text(encoding="utf-8")
        human_prompt = (
            "User request:\n{user_question}\n\n"
            "Chosen analysis:\n{analysis_name}\n\n"
            "Chosen time window:\n{time_window_label}\n\n"
            "Asset context:\n{asset_context}\n"
        )
        variables = {
            "user_question": user_question,
            "analysis_name": analysis_name,
            "time_window_label": time_window_label,
            "asset_context": self._clip_text(asset_context, 2600),
        }
        raw_response, total_tokens = self._invoke_text(
            system_prompt=system_prompt,
            human_prompt=human_prompt,
            variables=variables,
        )
        return {
            "sql_query": self._strip_code_fences(raw_response),
            "total_tokens": total_tokens,
            "raw_response": raw_response,
            "prompt_text": self._render_prompt_text(system_prompt, human_prompt, variables),
        }

    def analyze_result(
        self,
        user_question: str,
        asset_context: str,
        sql_query: str,
        deterministic_findings: dict[str, Any],
        rows: list[dict],
    ) -> dict[str, Any]:
        fallback = (
            "I retrieved the requested data, but the final LLM analysis could not be generated. "
            "Please review the SQL and database rows in the details panel."
        )
        system_prompt = self._result_analyst_path.read_text(encoding="utf-8")
        human_prompt = (
            "User request:\n{user_question}\n\n"
            "Asset context:\n{asset_context}\n\n"
            "SQL query used:\n{sql_query}\n\n"
            "Deterministic findings:\n{deterministic_findings}\n\n"
            "Returned rows:\n{rows}\n"
        )
        variables = {
            "user_question": user_question,
            "asset_context": self._clip_text(asset_context, 2400),
            "sql_query": self._clip_text(sql_query, 2200),
            "deterministic_findings": self._compact_findings(deterministic_findings),
            "rows": self._compact_rows(rows),
        }
        raw_response, total_tokens = self._invoke_text(
            system_prompt=system_prompt,
            human_prompt=human_prompt,
            variables=variables,
        )
        text = raw_response.strip() or fallback
        return {
            "answer": text,
            "total_tokens": total_tokens,
            "raw_response": raw_response,
            "prompt_text": self._render_prompt_text(system_prompt, human_prompt, variables),
        }

    def _invoke_json(
        self,
        system_prompt: str,
        human_prompt: str,
        variables: dict[str, Any],
        fallback: dict[str, Any],
    ) -> dict[str, Any]:
        raw_response, _ = self._invoke_text(system_prompt, human_prompt, variables)
        try:
            return json.loads(self._extract_json_object(raw_response))
        except Exception:
            return fallback

    def _invoke_text(
        self,
        system_prompt: str,
        human_prompt: str,
        variables: dict[str, Any],
    ) -> tuple[str, int]:
        if not self._settings.groq_api_key or ChatGroq is None:
            return "", 0

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                ("human", human_prompt),
            ]
        )

        llm = ChatGroq(
            api_key=self._settings.groq_api_key,
            model=self._settings.groq_model,
            temperature=0.1,
        )
        chain = prompt | llm
        response = chain.invoke(variables)
        raw_content = response.content if isinstance(response.content, str) else str(response.content)
        return raw_content, self._extract_total_tokens(response)

    def _extract_total_tokens(self, response: Any) -> int:
        usage_metadata = getattr(response, "usage_metadata", None) or {}
        response_metadata = getattr(response, "response_metadata", None) or {}
        token_usage = response_metadata.get("token_usage", {})

        if isinstance(usage_metadata, dict) and usage_metadata.get("total_tokens") is not None:
            return int(usage_metadata.get("total_tokens") or 0)
        if isinstance(token_usage, dict) and token_usage.get("total_tokens") is not None:
            return int(token_usage.get("total_tokens") or 0)
        return 0

    def _extract_json_object(self, text: str) -> str:
        fenced = re.search(r"```json\s*(\{.*\})\s*```", text, re.DOTALL)
        if fenced:
            return fenced.group(1)

        direct = re.search(r"(\{.*\})", text, re.DOTALL)
        if direct:
            return direct.group(1)

        raise ValueError("No JSON object found in LLM response.")

    def _strip_code_fences(self, text: str) -> str:
        cleaned = text.strip()
        cleaned = re.sub(r"^```sql\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^```\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        return cleaned.strip()

    def _render_prompt_text(
        self,
        system_prompt: str,
        human_prompt: str,
        variables: dict[str, Any],
    ) -> str:
        rendered_human = human_prompt.format(**variables)
        return f"[System Prompt]\n{system_prompt}\n\n[Human Prompt]\n{rendered_human}"

    def _clip_text(self, value: str, max_chars: int) -> str:
        text = (value or "").strip()
        if len(text) <= max_chars:
            return text
        return f"{text[:max_chars]} ...[truncated]"

    def _compact_findings(self, findings: dict[str, Any]) -> str:
        if not isinstance(findings, dict):
            return self._clip_text(json.dumps(findings, default=str), 1200)
        compact = {
            "status": findings.get("status", ""),
            "overview": findings.get("overview", ""),
            "findings": findings.get("findings", [])[:12],
            "warnings": findings.get("warnings", [])[:10],
            "numeric_metrics": findings.get("numeric_metrics", {}),
            "asset_specific": findings.get("asset_specific", {}),
            "table_excerpt": self._clip_text(str(findings.get("table", "")), 600),
        }
        return self._clip_text(json.dumps(compact, default=str), 1800)

    def _compact_rows(self, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "[]"

        # Keep representative context across child assets instead of only first rows.
        by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            asset_name = str(
                row.get("asset_name")
                or row.get("name")
                or row.get("device_name")
                or "unknown_asset"
            )
            by_asset[asset_name].append(row)

        selected_rows: list[dict[str, Any]] = []
        # Include latest row per asset first (group-wide visibility).
        for asset_name in sorted(by_asset.keys()):
            selected_rows.append(by_asset[asset_name][-1])

        # Include start-of-window row per asset to capture full-day behavior.
        for asset_name in sorted(by_asset.keys()):
            selected_rows.append(by_asset[asset_name][0])

        # Then include one midpoint row per asset when available.
        for asset_name in sorted(by_asset.keys()):
            entries = by_asset[asset_name]
            if len(entries) >= 3:
                selected_rows.append(entries[len(entries) // 2])
            if len(selected_rows) >= 60:
                break

        compacted: list[dict[str, Any]] = []
        important_keys = [
            "asset_name",
            "device_name",
            "name",
            "attribute_name",
            "sensor_name",
            "recorded_at",
            "timestamp",
            "latest_time",
            "warn_type",
            "current_level_ft",
            "percentage_filled",
            "tank_oil_level_in_feet_001",
            "max_ft",
            "temperature",
            "humidity",
            "bat_percent",
            "bat_voltage",
            "latest_temp",
            "latest_humidity",
            "lowest_temp",
            "highest_temp",
            "total_count",
            "shift_1_count",
            "shift_2_count",
            "shift_3_count",
            "status",
            "is_stale",
            "is_heartbeat_stale",
            "is_payload_stale",
            "level_change",
            "cooling_on_minutes",
        ]

        for row in selected_rows[:60]:
            compact_row: dict[str, Any] = {}
            for key in important_keys:
                if key not in row:
                    continue
                value = row.get(key)
                if isinstance(value, (list, dict)):
                    compact_row[key] = self._clip_text(json.dumps(value, default=str), 140)
                else:
                    compact_row[key] = self._clip_text(str(value), 140)
            if compact_row:
                compacted.append(compact_row)

        return self._clip_text(json.dumps(compacted, default=str), 3600)


llm_service = LLMService()
