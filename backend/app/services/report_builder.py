from __future__ import annotations

from pathlib import Path

from jinja2 import Template

from app.models.schemas import AnalysisPlan, AnalysisReport, AssetProfile
from app.services.deterministic_analysis import deterministic_analysis_service


class ReportBuilderService:
    def __init__(self) -> None:
        self._template_path = Path(__file__).resolve().parents[1] / "templates" / "report.html"

    def build(
        self,
        asset: AssetProfile,
        plan: AnalysisPlan,
        rows: list[dict],
        summary_override: str = "",
    ) -> AnalysisReport:
        title = f"{asset.name} - {plan.analysis_name}"
        summary = summary_override or self._summarize_rows(asset, plan, rows)
        markdown = self._to_markdown(title=title, asset=asset, plan=plan, rows=rows, summary=summary)
        html = self._to_html(title=title, asset=asset, plan=plan, rows=rows, summary=summary)
        return AnalysisReport(
            title=title,
            summary=summary,
            markdown=markdown,
            html=html,
        )

    def _summarize_rows(self, asset: AssetProfile, plan: AnalysisPlan, rows: list[dict]) -> str:
        if not rows:
            return (
                f"No rows were returned for {asset.name}. "
                f"The agent selected `{plan.analysis_name}` for {plan.time_window.label}."
            )

        metrics = ", ".join(asset.metrics) or "available telemetry"
        stats = deterministic_analysis_service.summarize(asset, rows)
        metric_lines = []
        for metric, values in list(stats["numeric_metrics"].items())[:3]:
            metric_lines.append(
                f"{metric}: latest {values['latest']:.2f}, min {values['min']:.2f}, "
                f"max {values['max']:.2f}, avg {values['avg']:.2f}"
            )
        findings = " ".join(stats["findings"][:3])
        stats_text = " ".join(metric_lines)
        return (
            f"{asset.name} was analyzed using `{plan.analysis_name}` over {plan.time_window.label}. "
            f"The report uses {len(rows)} rows and focuses on {metrics}. {findings} {stats_text}".strip()
        )

    def _to_markdown(
        self,
        title: str,
        asset: AssetProfile,
        plan: AnalysisPlan,
        rows: list[dict],
        summary: str,
    ) -> str:
        sample = rows[:5]
        lines = [
            f"# {title}",
            "",
            f"**Asset:** {asset.name}",
            f"**Asset type:** {asset.asset_type}",
            f"**Analysis:** {plan.analysis_name}",
            f"**Time window:** {plan.time_window.label}",
            "",
            "## Summary",
            summary,
            "",
            "## Reasoning",
            plan.reasoning,
            "",
            "## SQL",
            "```sql",
            plan.sql_query,
            "```",
            "",
            "## Sample Rows",
            "```json",
            str(sample),
            "```",
        ]
        return "\n".join(lines)

    def _to_html(
        self,
        title: str,
        asset: AssetProfile,
        plan: AnalysisPlan,
        rows: list[dict],
        summary: str,
    ) -> str:
        template = Template(self._template_path.read_text(encoding="utf-8"))
        columns = list(rows[0].keys()) if rows else ["message"]
        display_rows = rows[:10] if rows else [{"message": "No rows returned"}]
        return template.render(
            title=title,
            subtitle=f"{asset.asset_type} | {plan.time_window.label}",
            summary=summary,
            details=plan.reasoning,
            sql_query=plan.sql_query,
            columns=columns,
            rows=display_rows,
        )


report_builder_service = ReportBuilderService()
