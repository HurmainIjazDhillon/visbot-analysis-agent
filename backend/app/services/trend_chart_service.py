from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation

from app.models.schemas import AssetProfile, TimeWindow, TrendChart, TrendPoint, TrendSeries


class TrendChartService:
    _palette = [
        "#2563eb",
        "#0f766e",
        "#7c3aed",
        "#ea580c",
        "#dc2626",
        "#0891b2",
        "#1d4ed8",
        "#be123c",
        "#16a34a",
        "#7e22ce",
        "#0369a1",
        "#c2410c",
    ]

    def build(self, asset: AssetProfile, analysis_name: str, time_window: TimeWindow, rows: list[dict]) -> TrendChart | None:
        if not rows:
            return None

        time_key = self._time_key(rows)
        asset_key = "asset_name" if any("asset_name" in row for row in rows) else None
        series_keys = self._series_keys(asset, analysis_name, rows)
        if not series_keys:
          return None

        if time_key and self._has_multiple_labels(rows, time_key):
            return self._time_series_chart(asset, time_window, rows, time_key, asset_key, series_keys)
        return self._category_chart(asset, time_window, rows, asset_key, series_keys)

    def _time_series_chart(
        self,
        asset: AssetProfile,
        time_window: TimeWindow,
        rows: list[dict],
        time_key: str,
        asset_key: str | None,
        series_keys: list[str],
    ) -> TrendChart | None:
        grouped: dict[str, list[tuple[datetime | None, str, float]]] = defaultdict(list)
        asset_names = sorted(
            {
                str(row.get(asset_key)).strip()
                for row in rows
                if asset_key and row.get(asset_key)
            }
        )
        multiple_assets = len(asset_names) > 1
        selected_metric = series_keys[0]
        for row in rows:
            label = self._label_for_value(row.get(time_key))
            stamp = self._parse_datetime(row.get(time_key))
            prefix = str(row.get(asset_key)).strip() if asset_key and row.get(asset_key) else ""
            keys_to_use = [selected_metric] if multiple_assets else series_keys
            for key in keys_to_use:
                value = self._as_float(row.get(key))
                if value is None:
                    continue
                series_id = prefix if prefix else key
                grouped[series_id].append((stamp, label, value))

        if not grouped:
            return None

        if multiple_assets and all(len(values) <= 1 for values in grouped.values()):
            comparison_rows: list[dict] = []
            for series_id, values in grouped.items():
                if not values:
                    continue
                _, label, value = values[-1]
                comparison_rows.append(
                    {
                        "asset_name": series_id,
                        selected_metric: value,
                        "timestamp_label": label,
                    }
                )
            return self._category_chart(
                asset=asset,
                time_window=time_window,
                rows=comparison_rows,
                asset_key="asset_name",
                series_keys=[selected_metric],
            )

        ordered_items = sorted(grouped.items(), key=lambda item: item[0])

        series: list[TrendSeries] = []
        for index, (series_id, values) in enumerate(ordered_items):
            ordered_values = sorted(values, key=lambda item: item[0] or datetime.min)
            label = series_id if multiple_assets else self._humanize(selected_metric)
            series.append(
                TrendSeries(
                    key=series_id,
                    label=label,
                    color=self._palette[index % len(self._palette)],
                    points=[TrendPoint(label=point_label, value=value) for _, point_label, value in ordered_values],
                )
            )

        return TrendChart(
            title=f"{asset.name} trend graph",
            chart_type="line",
            x_label=time_window.label,
            y_label=self._humanize(selected_metric),
            summary="Trend graph generated directly from returned telemetry rows.",
            series=series,
        )

    def _category_chart(
        self,
        asset: AssetProfile,
        time_window: TimeWindow,
        rows: list[dict],
        asset_key: str | None,
        series_keys: list[str],
    ) -> TrendChart | None:
        label_key = asset_key or self._first_label_key(rows) or "row_label"
        selected_key = series_keys[0]
        
        # Aggregate the latest value for each label
        latest_values: dict[str, float] = {}
        for row_index, row in enumerate(rows):
            value = self._as_float(row.get(selected_key))
            if value is None:
                continue
            label = str(row.get(label_key) or f"Row {row_index + 1}")
            latest_values[label] = value

        points: list[TrendPoint] = [
            TrendPoint(label=label, value=value)
            for label, value in list(latest_values.items())[:24]
        ]

        if not points:
            return None

        return TrendChart(
            title=f"{asset.name} trend graph",
            chart_type="bar",
            x_label=self._humanize(label_key),
            y_label=self._humanize(selected_key),
            summary="Comparison chart generated directly from returned analysis rows.",
            series=[
                TrendSeries(
                    key=selected_key,
                    label=self._humanize(selected_key),
                    color=self._palette[0],
                    points=points,
                )
            ],
        )

    def _series_keys(self, asset: AssetProfile, analysis_name: str, rows: list[dict]) -> list[str]:
        priority_map = {
            "cold_room": ["temperature", "humidity"],
            "tank": ["current_level_ft", "percentage_filled", "tank_oil_level_in_feet_001", "level_change"],
            "filling_machine": ["total_count", "shift_1_count", "shift_2_count", "shift_3_count", "shift_1", "shift_2", "shift_3"],
            "smoke_alarm": ["smoke_detected", "temperature", "humidity", "bat_percent", "bat_voltage"],
            "report": ["latest_temp", "highest_temp", "lowest_temp", "total_count", "shift_1", "shift_2", "shift_3"],
            "energy_meter": ["energy_total", "power_a", "power_b", "power_c", "current_a", "current_b", "current_c"],
            "aqi": ["aqi_index", "pm25", "pm10", "co2", "voc"],
        }
        excluded = {"age_seconds", "is_stale", "is_previous_shift"}
        priority = priority_map.get(asset.asset_type, [])
        available = {key for row in rows for key in row.keys()}
        selected = [key for key in priority if key in available]
        if selected:
            return selected[:8]

        numeric_keys: list[str] = []
        for key in available:
            if key in excluded:
                continue
            if any(self._as_float(row.get(key)) is not None for row in rows):
                numeric_keys.append(key)
        numeric_keys.sort()
        return numeric_keys[:8]

    def _time_key(self, rows: list[dict]) -> str | None:
        candidates = ["recorded_at", "timestamp", "latest_time", "recorded_time", "date_recorded"]
        for key in candidates:
            if any(row.get(key) for row in rows):
                return key
        return None

    def _first_label_key(self, rows: list[dict]) -> str | None:
        candidates = ["asset_name", "name", "device_name", "status"]
        for key in candidates:
            if any(row.get(key) for row in rows):
                return key
        return None

    def _has_multiple_labels(self, rows: list[dict], key: str) -> bool:
        labels = {self._label_for_value(row.get(key)) for row in rows if row.get(key) is not None}
        return len(labels) > 1

    def _label_for_value(self, value: object) -> str:
        if isinstance(value, datetime):
            return value.strftime("%d %b %I:%M %p")
        text = str(value or "").strip()
        parsed = self._parse_datetime(text)
        if parsed is not None:
            return parsed.strftime("%d %b %I:%M %p")
        return text or "Unknown"

    def _parse_datetime(self, value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if not isinstance(value, str) or not value.strip():
            return None
        text = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
        for fmt in ("%d %b %Y, %I:%M:%S %p", "%d %b %Y, %I:%M %p", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _as_float(self, value: object) -> float | None:
        if isinstance(value, bool) or value is None:
            return None
        if isinstance(value, (int, float, Decimal)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(Decimal(value.strip()))
            except (InvalidOperation, ValueError):
                return None
        return None

    def _humanize(self, text: str) -> str:
        return text.replace("_", " ").replace("001", "001").strip().title()


trend_chart_service = TrendChartService()
