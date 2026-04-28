from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from app.models.schemas import (
    AnalysisPlan,
    AnalysisRequest,
    AnalysisResponse,
    ChatRequest,
    ChatResponse,
    TimeWindow,
)
from app.services.asset_analysis_instructions import build_asset_llm_context
from app.services.data_repository import DataQueryError, data_repository
from app.services.deterministic_analysis import deterministic_analysis_service
from app.services.live_asset_registry import live_asset_registry
from app.services.llm_service import llm_service
from app.services.nl_to_sql import nl_to_sql_service
from app.services.report_builder import report_builder_service
from app.services.sql_guard import validate_read_only_sql
from app.services.trend_chart_service import trend_chart_service


class AnalysisAgentService:
    async def run_chat(self, payload: ChatRequest) -> ChatResponse:
        from app.services.scheduling_agent import n8n_fast_path_service
        
        fast_path = n8n_fast_path_service.try_intercept(payload.message)
        if fast_path:
            return fast_path

        analysis_request = AnalysisRequest(
            asset_id=payload.asset_id,
            question=payload.message,
        )
        analysis = await self.run_analysis(analysis_request)
        return ChatResponse(answer=analysis.report.summary, analysis=analysis)

    async def run_analysis(self, payload: AnalysisRequest) -> AnalysisResponse:
        candidates = live_asset_registry.find_candidates(payload.question)
        direct_match = live_asset_registry.best_direct_match(payload.question)
        planner_result = self._default_planner_result()
        if direct_match is not None:
            planner_result["asset_id"] = direct_match.asset_id
            planner_result["reasoning"] = "Direct asset match found from the user message."
        elif payload.asset_id:
            planner_result["asset_id"] = payload.asset_id
            planner_result["reasoning"] = "Asset id was provided directly in request."
        else:
            planner_result = llm_service.plan_request(
                user_question=payload.question,
                asset_catalog_context=self._build_asset_catalog_context(candidates),
            )
        asset = self._resolve_asset(payload.asset_id, planner_result, payload.question, candidates)

        analysis_name = payload.analysis_name or planner_result.get("analysis_name") or "live_status"
        time_window = payload.time_window or self._parse_time_window(planner_result.get("time_window"))
        explicit_time_window = self._extract_explicit_time_range_window(payload.question)
        if explicit_time_window is not None:
            started_at, ended_at, label = explicit_time_window
            duration_hours = max(
                1,
                int(math.ceil((ended_at - started_at).total_seconds() / 3600)),
            )
            time_window = TimeWindow(
                scope="historical",
                value=duration_hours,
                unit="hours",
                label=label,
                start_at=started_at.isoformat(),
                end_at=ended_at.isoformat(),
            )
        elif (relative_day_window := self._extract_relative_day_window(payload.question)) is not None:
            started_at, ended_at, label = relative_day_window
            duration_hours = max(
                1,
                int(math.ceil((ended_at - started_at).total_seconds() / 3600)),
            )
            time_window = TimeWindow(
                scope="historical",
                value=duration_hours,
                unit="hours",
                label=label,
                start_at=started_at.isoformat(),
                end_at=ended_at.isoformat(),
            )
        elif (relative_duration_window := self._extract_relative_duration_window(payload.question)) is not None:
            time_window = relative_duration_window
        if explicit_time_window is None and (specific_date_window := self._extract_specific_date_window(payload.question)) is not None:
            started_at, ended_at, label = specific_date_window
            time_window = TimeWindow(
                scope="historical",
                value=1,
                unit="days",
                label=label,
                start_at=started_at.isoformat(),
                end_at=ended_at.isoformat(),
            )
        asset_context = build_asset_llm_context(asset)
        query_window = self._build_query_window(time_window)

        use_deterministic_sql = (
            asset.asset_type in {"tank", "cold_room", "smoke_alarm"}
            or (not asset.is_device and asset.child_count > 0)
        )
        sql_result = {"sql_query": "", "total_tokens": 0, "raw_response": ""}
        if use_deterministic_sql:
            sql_query = nl_to_sql_service.build_query(
                asset=asset,
                question=payload.question,
                analysis_name=analysis_name,
                time_window=time_window,
            ).query
        else:
            sql_result = llm_service.generate_sql(
                user_question=payload.question,
                asset_context=asset_context,
                analysis_name=analysis_name,
                time_window_label=time_window.label,
            )
            sql_query = nl_to_sql_service.normalize_generated_query(sql_result["sql_query"], asset)

        warnings: list[str] = []
        try:
            validate_read_only_sql(sql_query)
        except Exception:
            fallback_sql = nl_to_sql_service.build_query(
                asset=asset,
                question=payload.question,
                analysis_name=analysis_name,
                time_window=time_window,
            )
            sql_query = fallback_sql.query
            warnings.append("The LLM SQL was invalid, so a safe fallback SQL template was used.")

        try:
            rows = data_repository.execute_query(sql_query)
        except DataQueryError as exc:
            fallback_sql = nl_to_sql_service.build_query(
                asset=asset,
                question=payload.question,
                analysis_name=analysis_name,
                time_window=time_window,
            )
            sql_query = fallback_sql.query
            warnings.append("The generated SQL failed at runtime, so a safe fallback query was used.")
            try:
                rows = data_repository.execute_query(sql_query)
            except DataQueryError as retry_exc:
                raise HTTPException(status_code=502, detail=str(retry_exc)) from retry_exc
        rows = self._clip_rows_to_window(rows, time_window)
        latest_row_time = self._latest_row_timestamp(rows)
        query_window = self._apply_display_window_override(payload.question, time_window, query_window)

        deterministic_summary = deterministic_analysis_service.summarize(asset, rows)
        warnings.extend(list(deterministic_summary["warnings"]))
        if latest_row_time is not None:
            try:
                request_end = datetime.fromisoformat(query_window["ended_at_iso"])
                if latest_row_time.tzinfo is None and request_end.tzinfo is not None:
                    latest_row_time = latest_row_time.replace(tzinfo=request_end.tzinfo)
                if latest_row_time < request_end:
                    warnings.append(
                        f"Latest telemetry available in this window is {latest_row_time.strftime('%d %b %Y, %I:%M %p')} (Asia/Karachi)."
                    )
            except Exception:
                pass

        if asset.asset_type == "smoke_alarm":
            final_analysis = {
                "answer": self._build_smoke_alarm_answer(asset, time_window, deterministic_summary, rows),
                "total_tokens": 0,
                "raw_response": "",
                "prompt_text": "",
            }
        else:
            final_analysis = llm_service.analyze_result(
                user_question=payload.question,
                asset_context=asset_context,
                sql_query=sql_query,
                deterministic_findings=deterministic_summary,
                rows=rows,
            )

        total_tokens = (
            int(sql_result.get("total_tokens", 0))
            + int(final_analysis.get("total_tokens", 0))
        )

        plan = AnalysisPlan(
            analysis_name=analysis_name,
            reasoning=planner_result.get("reasoning", ""),
            time_window=time_window,
            query_started_at=query_window["started_at_iso"],
            query_ended_at=query_window["ended_at_iso"],
            query_window_label=query_window["label"],
            planner_prompt=planner_result.get("prompt_text", ""),
            sql_prompt=sql_result.get("prompt_text", ""),
            analyst_prompt=final_analysis.get("prompt_text", ""),
            sql_query=sql_query,
            chart_hints=self._chart_hints_for_analysis(analysis_name),
            warnings=warnings,
            total_tokens=total_tokens,
            llm_raw_response=final_analysis.get("raw_response", ""),
        )
        report = report_builder_service.build(
            asset=asset,
            plan=plan,
            rows=rows,
            summary_override=final_analysis.get("answer", ""),
        )
        trend_chart = trend_chart_service.build(asset, analysis_name, time_window, rows)
        trend_charts = [trend_chart] if trend_chart else []
        return AnalysisResponse(
            asset=asset,
            plan=plan,
            report=report,
            trend_chart=trend_chart,
            trend_charts=trend_charts,
            rows=rows,
        )

    def _build_smoke_alarm_answer(
        self,
        asset,
        time_window: TimeWindow,
        deterministic_summary: dict[str, object],
        rows: list[dict],
    ) -> str:
        findings = [str(item).strip() for item in deterministic_summary.get("findings", []) if str(item).strip()]
        warnings = [str(item).strip() for item in deterministic_summary.get("warnings", []) if str(item).strip()]

        if not rows:
            lines = [
                f"No smoke-alarm telemetry rows were found for {asset.name} in {time_window.label}.",
            ]
            if warnings:
                lines.append("")
                lines.extend(f"- {item}" for item in warnings[:4])
            return "\n".join(lines)

        headline = findings[0] if findings else f"{asset.name} smoke-alarm telemetry was analyzed for {time_window.label}."
        latest_states = [item for item in findings[1:] if ": " in item][:6]
        other_findings = [item for item in findings[1:] if item not in latest_states][:4]

        lines = [headline]

        if other_findings:
            lines.append("")
            lines.append("Key points")
            lines.extend(f"- {item}" for item in other_findings)

        if latest_states:
            lines.append("")
            lines.append("Latest device states")
            lines.extend(f"- {item}" for item in latest_states)

        if warnings:
            lines.append("")
            lines.append("Attention items")
            lines.extend(f"- {item}" for item in warnings[:4])

        return "\n".join(lines)

    def _apply_display_window_override(
        self,
        question: str,
        time_window: TimeWindow,
        query_window: dict[str, str],
    ) -> dict[str, str]:
        text = question.lower()
        # Keep window as-is unless user asked for "today".
        if "today" not in text:
            return query_window
        if not time_window.start_at:
            return query_window
        start_at = self._parse_datetime(time_window.start_at)
        if start_at is None:
            return query_window
        end_at = self._parse_datetime(query_window.get("ended_at_iso", ""))
        if end_at is None:
            return query_window

        friendly_format = "%d %b %Y, %I:%M %p"
        query_window["label"] = (
            f"{start_at.strftime(friendly_format)} to "
            f"{end_at.strftime(friendly_format)} "
            f"(Asia/Karachi)"
        )
        return query_window

    def _latest_row_timestamp(self, rows: list[dict]) -> datetime | None:
        timestamp_keys = ("recorded_at", "timestamp", "local_time", "raw_time", "latest_time")
        latest: datetime | None = None
        for row in rows:
            for key in timestamp_keys:
                value = row.get(key)
                if value is None:
                    continue
                parsed = self._parse_datetime(value)
                if parsed is None:
                    continue
                if latest is None or parsed > latest:
                    latest = parsed
                break
        return latest

    def _resolve_asset(
        self,
        requested_asset_id: str | None,
        planner_result: dict,
        question: str,
        candidates: list,
    ) -> object:
        if requested_asset_id:
            asset = live_asset_registry.get_asset(requested_asset_id)
            if asset is not None:
                return asset

        direct_match = live_asset_registry.best_direct_match(question)
        if direct_match is not None:
            return direct_match

        planned_asset_id = planner_result.get("asset_id", "")
        if planned_asset_id:
            asset = live_asset_registry.get_asset(planned_asset_id)
            if asset is not None:
                return asset

        available = list(dict.fromkeys(asset.name for asset in (candidates[:12] or live_asset_registry.list_assets()[:12])))
        detail = "I could not identify the asset from your message."
        if available:
            detail += f" Available assets include: {', '.join(available)}."
        raise HTTPException(status_code=422, detail=detail)

    def _parse_time_window(self, raw_time_window: dict | None) -> TimeWindow:
        if not raw_time_window:
            return TimeWindow(scope="historical", value=3, unit="hours", label="last 3 hours")
        try:
            return TimeWindow(**raw_time_window)
        except Exception:
            return TimeWindow(scope="historical", value=3, unit="hours", label="last 3 hours")

    def _build_query_window(self, time_window: TimeWindow) -> dict[str, str]:
        timezone = ZoneInfo("Asia/Karachi")
        if time_window.start_at and time_window.end_at:
            started_at = datetime.fromisoformat(time_window.start_at)
            ended_at = datetime.fromisoformat(time_window.end_at)
            friendly_format = "%d %b %Y, %I:%M %p"
            return {
                "started_at_iso": started_at.isoformat(),
                "ended_at_iso": ended_at.isoformat(),
                "label": (
                    f"{started_at.strftime(friendly_format)} to "
                    f"{ended_at.strftime(friendly_format)} (Asia/Karachi)"
                ),
            }

        ended_at = datetime.now(timezone)

        unit_to_delta = {
            "minutes": timedelta(minutes=time_window.value),
            "hours": timedelta(hours=time_window.value),
            "days": timedelta(days=time_window.value),
            "weeks": timedelta(weeks=time_window.value),
            "months": timedelta(days=30 * time_window.value),
        }
        started_at = ended_at - unit_to_delta.get(time_window.unit, timedelta(hours=3))
        friendly_format = "%d %b %Y, %I:%M %p"

        return {
            "started_at_iso": started_at.isoformat(),
            "ended_at_iso": ended_at.isoformat(),
            "label": (
                f"{started_at.strftime(friendly_format)} to "
                f"{ended_at.strftime(friendly_format)} (Asia/Karachi)"
            ),
        }

    def _extract_specific_date_window(self, question: str) -> tuple[datetime, datetime, str] | None:
        timezone = ZoneInfo("Asia/Karachi")
        patterns = [
            r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b",
            r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b",
            r"\b(\d{1,2})-(\d{1,2})-(\d{4})\b",
        ]

        for pattern in patterns:
            match = re.search(pattern, question)
            if not match:
                continue
            groups = match.groups()
            try:
                if pattern.startswith(r"\b(\d{4})"):
                    year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                else:
                    day, month, year = int(groups[0]), int(groups[1]), int(groups[2])
                anchor_date = datetime(year, month, day, 6, 0, 0, tzinfo=timezone)
                end_date = anchor_date + timedelta(days=1)
                label = (
                    f"{anchor_date.strftime('%d %b %Y')} 06:00 AM to "
                    f"{end_date.strftime('%d %b %Y')} 06:00 AM"
                )
                return anchor_date, end_date, label
            except ValueError:
                continue

        month_name_patterns = [
            r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s*,?\s*(\d{4})\b",
            r"\b([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?\s*,?\s*(\d{4})\b",
            r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\b",
            r"\b([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?\b",
        ]
        month_map = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "decebmber": 12, "december": 12,
        }
        now_local = datetime.now(timezone)
        for pattern in month_name_patterns:
            match = re.search(pattern, question, flags=re.IGNORECASE)
            if not match:
                continue
            groups = match.groups()
            try:
                if pattern.startswith(r"\b(\d"):
                    day = int(groups[0])
                    month = month_map.get(groups[1].lower())
                    year = int(groups[2]) if len(groups) > 2 and groups[2] else now_local.year
                else:
                    month = month_map.get(groups[0].lower())
                    day = int(groups[1])
                    year = int(groups[2]) if len(groups) > 2 and groups[2] else now_local.year
                if month is None:
                    continue
                anchor_date = datetime(year, month, day, 6, 0, 0, tzinfo=timezone)
                end_date = anchor_date + timedelta(days=1)
                label = (
                    f"{anchor_date.strftime('%d %b %Y')} 06:00 AM to "
                    f"{end_date.strftime('%d %b %Y')} 06:00 AM"
                )
                return anchor_date, end_date, label
            except ValueError:
                continue

        ordinal_only = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)\b", question, flags=re.IGNORECASE)
        if ordinal_only:
            day = int(ordinal_only.group(1))
            year = now_local.year
            month = now_local.month
            if day > now_local.day:
                if month == 1:
                    month = 12
                    year -= 1
                else:
                    month -= 1
            try:
                anchor_date = datetime(year, month, day, 6, 0, 0, tzinfo=timezone)
                end_date = anchor_date + timedelta(days=1)
                label = (
                    f"{anchor_date.strftime('%d %b %Y')} 06:00 AM to "
                    f"{end_date.strftime('%d %b %Y')} 06:00 AM"
                )
                return anchor_date, end_date, label
            except ValueError:
                return None
        return None

    def _extract_relative_duration_window(self, question: str) -> TimeWindow | None:
        lowered = question.lower()

        unit_map = {
            "h": "hours",
            "hr": "hours",
            "hrs": "hours",
            "hour": "hours",
            "hours": "hours",
            "d": "days",
            "day": "days",
            "days": "days",
            "w": "weeks",
            "wk": "weeks",
            "wks": "weeks",
            "week": "weeks",
            "weeks": "weeks",
            "m": "months",
            "mo": "months",
            "mon": "months",
            "month": "months",
            "months": "months",
        }

        def build_window(value: int, raw_unit: str) -> TimeWindow | None:
            unit = unit_map.get(raw_unit.lower())
            if unit is None:
                return None
            return TimeWindow(
                scope="historical",
                value=value,
                unit=unit,
                label=f"last {value} {unit}",
            )

        pattern = r"\b(?:last|past|for|previous|in)\s+(\d+)\s*(h|hr|hrs?|hours?|d|days?|w|wk|wks|weeks?|m|mo|mon|months?)\b"
        match = re.search(pattern, lowered)
        if match:
            built = build_window(int(match.group(1)), match.group(2))
            if built is not None:
                return built

        pattern_single = r"\b(?:last|past|for|previous|in)\s+(?:an?|1)\s+(h|hr|hrs?|hours?|d|days?|w|wk|wks|weeks?|m|mo|mon|months?)\b"
        match_single = re.search(pattern_single, lowered)
        if match_single:
            built = build_window(1, match_single.group(1))
            if built is not None:
                return built

        pattern_bare = r"\b(\d+)\s*(h|hr|hrs?|hours?|d|days?|w|wk|wks|weeks?|m|mo|mon|months?)\b"
        match_bare = re.search(pattern_bare, lowered)
        if match_bare:
            built = build_window(int(match_bare.group(1)), match_bare.group(2))
            if built is not None:
                return built

        return None

    def _extract_explicit_time_range_window(self, question: str) -> tuple[datetime, datetime, str] | None:
        timezone = ZoneInfo("Asia/Karachi")
        lowered = question.lower()
        range_match = re.search(
            r"\b(?:from\s+)?(\d{1,2}(?::\d{2})?\s*(?:am|pm))\s*(?:to|-|till|until)\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm))\b",
            lowered,
            flags=re.IGNORECASE,
        )
        if not range_match:
            return None

        start_hm = self._parse_clock_token(range_match.group(1))
        end_hm = self._parse_clock_token(range_match.group(2))
        if start_hm is None or end_hm is None:
            return None

        anchor = self._extract_anchor_date(question, timezone) or datetime.now(timezone).date()
        started_at = datetime(anchor.year, anchor.month, anchor.day, start_hm[0], start_hm[1], tzinfo=timezone)
        ended_at = datetime(anchor.year, anchor.month, anchor.day, end_hm[0], end_hm[1], tzinfo=timezone)
        if ended_at <= started_at:
            ended_at += timedelta(days=1)

        friendly_format = "%d %b %Y, %I:%M %p"
        label = f"{started_at.strftime(friendly_format)} to {ended_at.strftime(friendly_format)} (Asia/Karachi)"
        return started_at, ended_at, label

    def _parse_clock_token(self, token: str) -> tuple[int, int] | None:
        match = re.match(r"^\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*$", token.strip(), flags=re.IGNORECASE)
        if not match:
            return None
        hour = int(match.group(1))
        minute = int(match.group(2) or "0")
        meridian = match.group(3).lower()
        if hour < 1 or hour > 12 or minute < 0 or minute > 59:
            return None
        if meridian == "am":
            hour = 0 if hour == 12 else hour
        else:
            hour = 12 if hour == 12 else hour + 12
        return hour, minute

    def _extract_anchor_date(self, question: str, timezone: ZoneInfo) -> date | None:
        patterns = [
            r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b",
            r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b",
            r"\b(\d{1,2})-(\d{1,2})-(\d{4})\b",
        ]

        for pattern in patterns:
            match = re.search(pattern, question)
            if not match:
                continue
            groups = match.groups()
            try:
                if pattern.startswith(r"\b(\d{4})"):
                    year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                else:
                    day, month, year = int(groups[0]), int(groups[1]), int(groups[2])
                return date(year, month, day)
            except ValueError:
                continue

        month_name_patterns = [
            r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s*,?\s*(\d{4})\b",
            r"\b([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?\s*,?\s*(\d{4})\b",
            r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\b",
            r"\b([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?\b",
        ]
        month_map = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "decebmber": 12, "december": 12,
        }
        now_local = datetime.now(timezone)
        for pattern in month_name_patterns:
            match = re.search(pattern, question, flags=re.IGNORECASE)
            if not match:
                continue
            groups = match.groups()
            try:
                if pattern.startswith(r"\b(\d"):
                    day = int(groups[0])
                    month = month_map.get(groups[1].lower())
                    year = int(groups[2]) if len(groups) > 2 and groups[2] else now_local.year
                else:
                    month = month_map.get(groups[0].lower())
                    day = int(groups[1])
                    year = int(groups[2]) if len(groups) > 2 and groups[2] else now_local.year
                if month is None:
                    continue
                return date(year, month, day)
            except ValueError:
                continue

        ordinal_only = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)\b", question, flags=re.IGNORECASE)
        if ordinal_only:
            day = int(ordinal_only.group(1))
            year = now_local.year
            month = now_local.month
            if day > now_local.day:
                if month == 1:
                    month = 12
                    year -= 1
                else:
                    month -= 1
            try:
                return date(year, month, day)
            except ValueError:
                return None
        return None

    def _extract_relative_day_window(self, question: str) -> tuple[datetime, datetime, str] | None:
        text = question.lower()
        timezone = ZoneInfo("Asia/Karachi")
        now_local = datetime.now(timezone)

        if "today" in text:
            shift_start = now_local.replace(hour=6, minute=0, second=0, microsecond=0)
            if now_local < shift_start:
                shift_start = shift_start - timedelta(days=1)
            label = (
                f"{shift_start.strftime('%d %b %Y')} 06:00 AM to "
                f"{now_local.strftime('%d %b %Y %I:%M %p')}"
            )
            return shift_start, now_local, label

        if "yesterday" in text:
            today_shift_start = now_local.replace(hour=6, minute=0, second=0, microsecond=0)
            if now_local < today_shift_start:
                today_shift_start = today_shift_start - timedelta(days=1)
            yesterday_shift_start = today_shift_start - timedelta(days=1)
            label = (
                f"{yesterday_shift_start.strftime('%d %b %Y')} 06:00 AM to "
                f"{today_shift_start.strftime('%d %b %Y')} 06:00 AM"
            )
            return yesterday_shift_start, today_shift_start, label

        return None

    def _clip_rows_to_window(self, rows: list[dict], time_window: TimeWindow) -> list[dict]:
        if not rows or not time_window.start_at or not time_window.end_at:
            return rows
        try:
            start_dt = datetime.fromisoformat(time_window.start_at)
            end_dt = datetime.fromisoformat(time_window.end_at)
        except ValueError:
            return rows

        timestamp_keys = ("recorded_at", "timestamp", "local_time", "raw_time")
        filtered: list[dict] = []
        for row in rows:
            row_ts = None
            for key in timestamp_keys:
                value = row.get(key)
                if value is None:
                    continue
                row_ts = self._parse_datetime(value)
                if row_ts is not None:
                    break
            if row_ts is None:
                filtered.append(row)
                continue

            if row_ts.tzinfo is None and start_dt.tzinfo is not None:
                row_ts = row_ts.replace(tzinfo=start_dt.tzinfo)
            if start_dt <= row_ts < end_dt:
                filtered.append(row)
        return filtered

    def _parse_datetime(self, value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            pass
        for fmt in ("%d-%m-%Y %I:%M %p", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _default_planner_result(self) -> dict:
        return {
            "asset_id": "",
            "analysis_name": "live_status",
            "time_window": {
                "scope": "historical",
                "value": 3,
                "unit": "hours",
                "label": "last 3 hours",
            },
            "reasoning": "Planner bypassed.",
            "prompt_text": "[BYPASSED]",
        }

    def _build_asset_catalog_context(self, candidates: list) -> str:
        shortlisted = candidates[:4] if candidates else live_asset_registry.list_assets()[:5]
        entries: list[str] = []
        total_chars = 0
        max_chars = 2400
        for asset in shortlisted:
            aliases = [
                alias for alias in asset.lookup_names
                if alias and alias.lower() != asset.name.lower()
            ][:2]
            entry = json.dumps(
                {
                    "asset_id": asset.asset_id,
                    "name": asset.name,
                    "aliases": aliases,
                    "type": asset.asset_type,
                    "parent": asset.parent_name or "",
                    "child_count": asset.child_count,
                    "has_live_data": asset.has_live_data,
                    "data_keys": asset.data_keys[:4],
                    "supported_analyses": asset.supported_analyses[:3],
                }
            )
            if total_chars + len(entry) > max_chars:
                break
            entries.append(entry)
            total_chars += len(entry)
        return "\n".join(entries)

    def _chart_hints_for_analysis(self, analysis_name: str) -> list[str]:
        mapping = {
            "cooling_cycle_analysis": ["line_chart", "event_markers"],
            "consumption_analysis": ["line_chart", "duration_summary", "event_markers"],
            "live_status": ["stat_cards"],
            "trend_analysis": ["line_chart"],
            "historical_variance": ["line_chart", "box_plot"],
            "throughput_utilization": ["bar_chart", "line_chart"],
            "inventory_level_analysis": ["line_chart", "threshold_bands"],
            "anomaly_detection": ["line_chart", "anomaly_table"],
        }
        return mapping.get(analysis_name, ["line_chart"])


analysis_agent_service = AnalysisAgentService()
