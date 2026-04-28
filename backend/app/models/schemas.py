from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


AnalysisScope = Literal["live", "historical"]
WindowUnit = Literal["minutes", "hours", "days", "weeks", "months"]


class TimeWindow(BaseModel):
    scope: AnalysisScope = "live"
    value: int = 1
    unit: WindowUnit = "hours"
    label: str = "last 1 hour"
    start_at: str | None = None
    end_at: str | None = None


class AssetProfile(BaseModel):
    asset_id: str
    name: str
    asset_type: str
    description: str
    db_asset_id: str = ""
    parent_name: str = ""
    child_count: int = 0
    is_device: bool = False
    attribute_keys: list[str] = Field(default_factory=list)
    has_live_data: bool = False
    data_keys: list[str] = Field(default_factory=list)
    supported_analyses: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    source_table: str = ""
    analysis_instructions: str = ""
    sql_notes: str = ""
    lookup_names: list[str] = Field(default_factory=list)
    temperature_keys: list[str] = Field(default_factory=list)
    humidity_keys: list[str] = Field(default_factory=list)
    level_keys: list[str] = Field(default_factory=list)
    production_keys: list[str] = Field(default_factory=list)
    nominal_range: dict[str, float] = Field(default_factory=dict)


class AnalysisPlan(BaseModel):
    analysis_name: str
    reasoning: str
    time_window: TimeWindow
    query_started_at: str = ""
    query_ended_at: str = ""
    query_window_label: str = ""
    planner_prompt: str = ""
    sql_prompt: str = ""
    analyst_prompt: str = ""
    sql_query: str
    chart_hints: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    total_tokens: int = 0
    llm_raw_response: str = ""


class AnalysisReport(BaseModel):
    report_id: str = Field(default_factory=lambda: str(uuid4()))
    title: str
    summary: str
    markdown: str
    html: str
    created_at: datetime = Field(default_factory=datetime.utcnow)


class AnalysisRequest(BaseModel):
    asset_id: str | None = None
    question: str
    analysis_name: str | None = None
    time_window: TimeWindow | None = None


class TrendPoint(BaseModel):
    label: str
    value: float


class TrendSeries(BaseModel):
    key: str
    label: str
    color: str
    points: list[TrendPoint] = Field(default_factory=list)


class TrendChart(BaseModel):
    title: str
    chart_type: Literal["line", "bar"]
    x_label: str
    y_label: str
    summary: str = ""
    series: list[TrendSeries] = Field(default_factory=list)


class AnalysisResponse(BaseModel):
    asset: AssetProfile
    plan: AnalysisPlan
    report: AnalysisReport
    trend_chart: TrendChart | None = None
    trend_charts: list[TrendChart] = Field(default_factory=list)
    rows: list[dict] = Field(default_factory=list)


class ChatRequest(BaseModel):
    message: str
    asset_id: str | None = None


class ChatResponse(BaseModel):
    answer: str
    analysis: AnalysisResponse
