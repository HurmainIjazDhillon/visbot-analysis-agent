from __future__ import annotations

from decimal import Decimal, InvalidOperation
from datetime import datetime, time
from statistics import mean
from collections import defaultdict

from app.models.schemas import AssetProfile


class DeterministicAnalysisService:
    def summarize(self, asset: AssetProfile, rows: list[dict]) -> dict[str, object]:
        base = self._generic_summary(rows)
        domain_summary = self._asset_specific_summary(asset, rows)
        base["warnings"].extend(domain_summary["warnings"])
        base["findings"] = domain_summary["findings"]
        return base

    def _generic_summary(self, rows: list[dict]) -> dict[str, object]:
        if not rows:
            return {
                "numeric_metrics": {},
                "warnings": ["No telemetry rows were returned for the requested window."],
                "findings": [],
            }

        numeric_columns: dict[str, list[float]] = {}
        for row in rows:
            for key, value in row.items():
                if isinstance(value, bool):
                    continue
                numeric_value = self._as_float(value)
                if numeric_value is not None:
                    numeric_columns.setdefault(key, []).append(numeric_value)

        metric_summary: dict[str, dict[str, float]] = {}
        warnings: list[str] = []

        for column, values in numeric_columns.items():
            if not values:
                continue
            avg = mean(values)
            metric_summary[column] = {
                "min": min(values),
                "max": max(values),
                "avg": round(avg, 3),
                "latest": values[-1],
            }

            if len(values) > 1 and avg != 0:
                spread = max(values) - min(values)
                if spread > abs(avg) * 0.2:
                    warnings.append(
                        f"{column} changed noticeably across the requested window "
                        f"(min={min(values):.2f}, max={max(values):.2f})."
                    )

        return {
            "numeric_metrics": metric_summary,
            "warnings": warnings,
            "findings": self._generic_trend_findings(metric_summary),
        }

    def _asset_specific_summary(self, asset: AssetProfile, rows: list[dict]) -> dict[str, object]:
        if asset.asset_type == "cold_room":
            return self._cold_room_summary(asset, rows)
        if asset.asset_type == "filling_machine":
            return self._filling_machine_summary(rows)
        if asset.asset_type == "tank":
            return self._tank_summary(asset, rows)
        if asset.asset_type == "smoke_alarm":
            return self._smoke_alarm_summary(asset, rows)
        return {"warnings": [], "findings": []}

    def _smoke_alarm_summary(self, asset: AssetProfile, rows: list[dict]) -> dict[str, object]:
        findings: list[str] = []
        warnings: list[str] = []

        per_asset: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            asset_name = str(row.get("asset_name") or asset.name or "Unknown")
            per_asset[asset_name].append(row)

        if not per_asset:
            return {"warnings": ["No smoke-alarm telemetry rows were available for the requested window."], "findings": []}

        smoke_events: list[tuple[str, datetime | None, str]] = []
        muted_events: list[tuple[str, datetime | None, str]] = []
        fault_events: list[tuple[str, datetime | None, str]] = []
        remove_events: list[tuple[str, datetime | None, str]] = []
        low_voltage_assets: set[str] = set()
        unknown_assets: set[str] = set()
        latest_states: list[str] = []

        for asset_name, asset_rows in sorted(per_asset.items()):
            ordered = sorted(asset_rows, key=lambda row: self._parse_datetime(row.get("recorded_at") or row.get("timestamp")) or datetime.min)
            per_sensor: dict[str, list[dict]] = defaultdict(list)
            for row in ordered:
                sensor_name = str(row.get("attribute_name") or row.get("sensor_name") or "sensor")
                per_sensor[sensor_name].append(row)

            for row in ordered:
                warn_type = self._normalize_warn_type(row.get("warn_type"))
                event_time = self._parse_datetime(row.get("recorded_at") or row.get("timestamp"))
                attribute_name = str(row.get("attribute_name") or row.get("sensor_name") or "sensor")
                event_ref = (asset_name, event_time, attribute_name)
                if warn_type == "warn":
                    smoke_events.append(event_ref)
                elif warn_type == "mute":
                    muted_events.append(event_ref)
                elif warn_type == "fault":
                    fault_events.append(event_ref)
                elif warn_type == "remove":
                    remove_events.append(event_ref)
                elif warn_type in {"low-vol", "low-vol-test"}:
                    low_voltage_assets.add(asset_name)
                elif warn_type == "unknown":
                    unknown_assets.add(asset_name)

            latest_sensor_summaries: list[str] = []
            for sensor_name, sensor_rows in sorted(per_sensor.items()):
                latest_row = sensor_rows[-1]
                latest_warn = self._normalize_warn_type(latest_row.get("warn_type"))
                latest_time = self._parse_datetime(latest_row.get("recorded_at") or latest_row.get("timestamp"))
                latest_temp = self._as_float(latest_row.get("temperature"))
                latest_humidity = self._as_float(latest_row.get("humidity"))
                latest_battery = self._as_float(latest_row.get("bat_percent"))

                raw_data = latest_row.get("sensor_data")
                if raw_data is None and latest_warn is None:
                    # Explicit handling for assets with no data in the window
                    state_fragments = [f"{sensor_name}: No recent telemetry found (Offline)"]
                    latest_sensor_summaries.append(" ".join(state_fragments))
                    continue

                state_fragments = [f"{sensor_name}: {latest_warn or 'ok'}"]
                if latest_time is not None:
                    state_fragments.append(f"at {latest_time.strftime('%d %b %Y, %I:%M %p')}")
                
                details: list[str] = []
                if latest_temp is not None:
                    details.append(f"{latest_temp:.1f}C")
                if latest_humidity is not None:
                    details.append(f"{latest_humidity:.1f}% RH")
                if latest_battery is not None:
                    details.append(f"battery {latest_battery:.0f}%")

                if isinstance(raw_data, dict):
                    skip_keys = {"warn", "temp", "humi", "bat_percent", "bat_voltage", "name", "timestamp"}
                    for k, v in raw_data.items():
                        if k.lower() in skip_keys:
                            continue
                        val = self._as_float(v)
                        if val is not None:
                            details.append(f"{k}: {val}")
                        elif isinstance(v, str) and len(v) < 20:
                            details.append(f"{k}: {v}")
                else:
                    # If it's a simple value, include it directly
                    val = self._as_float(raw_data)
                    if val is not None:
                        details.append(f"value: {val}")
                    elif raw_data is not None:
                        details.append(f"value: {raw_data}")

                if details:
                    state_fragments.append(f"({', '.join(details)})")
                
                # If everything is OK and it's not a smoke event, keep it to one line
                if not latest_warn or latest_warn == 'ok':
                    latest_sensor_summaries.append(" ".join(state_fragments[:1]))
                else:
                    latest_sensor_summaries.append(" ".join(state_fragments))

            if latest_sensor_summaries:
                latest_states.append(f"{asset_name}: {'; '.join(latest_sensor_summaries)}")

        if smoke_events:
            preview = ", ".join(
                f"{name} at {(event_time.strftime('%d %b %I:%M %p') if event_time else 'unknown time')}"
                for name, event_time, _ in smoke_events[:6]
            )
            findings.append(f"Smoke/fire was detected in {len(smoke_events)} event(s): {preview}.")
        else:
            findings.append("No smoke/fire detection event was found in the returned telemetry window.")

        if muted_events:
            preview = ", ".join(
                f"{name} at {(event_time.strftime('%d %b %I:%M %p') if event_time else 'unknown time')}"
                for name, event_time, _ in muted_events[:6]
            )
            findings.append(f"Alarm mute events were recorded for {len(muted_events)} event(s): {preview}.")

        if fault_events:
            preview = ", ".join(
                f"{name} at {(event_time.strftime('%d %b %I:%M %p') if event_time else 'unknown time')}"
                for name, event_time, _ in fault_events[:6]
            )
            warnings.append(f"Fault events were detected on {len(fault_events)} row(s): {preview}.")

        if remove_events:
            preview = ", ".join(
                f"{name} at {(event_time.strftime('%d %b %I:%M %p') if event_time else 'unknown time')}"
                for name, event_time, _ in remove_events[:6]
            )
            warnings.append(f"Sensor removal events were detected: {preview}.")

        if low_voltage_assets:
            warnings.append(
                f"Low-voltage/battery alerts appeared on {len(low_voltage_assets)} device(s): "
                + ", ".join(sorted(low_voltage_assets)[:8])
                + ("." if len(low_voltage_assets) <= 8 else " ...")
            )

        if unknown_assets:
            warnings.append(
                f"Unknown warn states were reported by {len(unknown_assets)} device(s): "
                + ", ".join(sorted(unknown_assets))
                + "."
            )

        findings.extend(latest_states)
        return {"warnings": warnings, "findings": findings}

    def _cold_room_summary(self, asset: AssetProfile, rows: list[dict]) -> dict[str, object]:
        findings: list[str] = []
        warnings: list[str] = []
        temps = [
            numeric
            for row in rows
            if (numeric := self._as_float(row.get("temperature"))) is not None
        ]
        stale = any(bool(row.get("is_stale")) for row in rows)
        min_ok = asset.nominal_range.get("min_temperature", 5.0)
        max_ok = asset.nominal_range.get("max_temperature", 45.0)

        if temps:
            latest = temps[0]
            earliest = temps[-1]
            findings.append(f"Latest temperature is {latest:.2f}C.")
            findings.append(f"Observed range is {min(temps):.2f}C to {max(temps):.2f}C.")
            findings.append(
                f"Temperature trend over the window is {self._describe_trend(latest - earliest, 'C')}."
            )
            
            # Use 20.0 as hardcoded cycle threshold if not specified, 
            # but don't use it for nominal range warnings.
            cycle_threshold = max_ok if max_ok is not None else 20.0
            cycles = self._estimate_cooling_cycles(temps, threshold=cycle_threshold)
            findings.append(f"Estimated cooling cycle count in this window: {cycles}.")
            
            consumption_summary = self._cooling_consumption_summary(rows)
            if consumption_summary:
                findings.extend(consumption_summary["findings"])
                warnings.extend(consumption_summary["warnings"])
            
            if min_ok is not None and max_ok is not None:
                if latest < min_ok or latest > max_ok:
                    warnings.append(
                        f"Latest temperature {latest:.2f}C is outside the nominal range of {min_ok:.2f}C to {max_ok:.2f}C."
                    )
                if max(temps) > max_ok:
                    warnings.append(f"Temperature crossed the upper threshold of {max_ok:.2f}C.")

        if stale:
            warnings.append("The most recent cold room payload appears stale.")

        return {"warnings": warnings, "findings": findings}

    def _filling_machine_summary(self, rows: list[dict]) -> dict[str, object]:
        findings: list[str] = []
        warnings: list[str] = []
        totals = [
            numeric
            for row in rows
            if (numeric := self._as_float(row.get("total_count"))) is not None
        ]
        stale = any(bool(row.get("is_stale")) for row in rows)

        if totals:
            latest = totals[0]
            earliest = totals[-1]
            findings.append(f"Latest total production count is {latest:.0f}.")
            findings.append(f"Net production movement in this window is {latest - earliest:.0f}.")
            findings.append(
                f"Production trend over the window is {self._describe_trend(latest - earliest, 'units', precision=0)}."
            )
            if latest == earliest and len(totals) > 1:
                warnings.append("Production total did not change during the requested window.")

        shift_columns = ["shift_1_count", "shift_2_count", "shift_3_count"]
        shift_values = {
            column: numeric
            for column in shift_columns
            if rows and (numeric := self._as_float(rows[0].get(column))) is not None
        }
        if shift_values:
            best_shift = max(shift_values, key=shift_values.get)
            findings.append(f"The strongest current shift metric is {best_shift} at {shift_values[best_shift]:.0f}.")

        if stale:
            warnings.append("The filling machine payload timestamp appears stale.")

        return {"warnings": warnings, "findings": findings}

    def _tank_summary(self, asset: AssetProfile, rows: list[dict]) -> dict[str, object]:
        findings: list[str] = []
        warnings: list[str] = []
        low_level_threshold = asset.nominal_range.get("low_level_threshold", 3.0)

        per_asset_points: dict[str, list[tuple[datetime, float, float | None, float | None]]] = defaultdict(list)
        for row in rows:
            level = self._as_float(row.get("current_level_ft"))
            if level is None:
                continue
            raw_asset_name = row.get("asset_name") or row.get("name") or "Unknown"
            asset_name = str(raw_asset_name)
            timestamp = (
                self._parse_datetime(row.get("recorded_at"))
                or self._parse_datetime(row.get("timestamp"))
                or self._parse_datetime(row.get("latest_time"))
                or datetime.min
            )
            pct = self._as_float(row.get("percentage_filled"))
            max_ft = self._as_float(row.get("max_ft"))
            per_asset_points[asset_name].append((timestamp, level, pct, max_ft))

        if not per_asset_points:
            return {"warnings": ["No valid tank level points were available for deterministic analysis."], "findings": []}

        per_asset_stats: list[dict[str, float | str | None]] = []
        missing_capacity_assets: set[str] = set()
        for asset_name, points in per_asset_points.items():
            ordered = sorted(points, key=lambda item: item[0])
            start_level = ordered[0][1]
            end_level = ordered[-1][1]
            min_level = min(point[1] for point in ordered)
            max_level = max(point[1] for point in ordered)
            pct_candidates = [point[2] for point in ordered if point[2] is not None]
            max_ft_candidates = [point[3] for point in ordered if point[3] is not None]
            if not max_ft_candidates:
                missing_capacity_assets.add(asset_name)
            end_pct = pct_candidates[-1] if pct_candidates else None
            per_asset_stats.append(
                {
                    "asset_name": asset_name,
                    "start_level": start_level,
                    "end_level": end_level,
                    "min_level": min_level,
                    "max_level": max_level,
                    "net_change": end_level - start_level,
                    "end_pct": end_pct,
                }
            )

        per_asset_stats.sort(key=lambda item: str(item["asset_name"]))
        tank_count = len(per_asset_stats)
        low_assets = [item for item in per_asset_stats if float(item["end_level"]) <= low_level_threshold]
        depleted_assets = [item for item in per_asset_stats if float(item["net_change"]) < 0]
        refilled_assets = [item for item in per_asset_stats if float(item["net_change"]) > 0]

        findings.append(f"Analyzed {tank_count} tank assets across the requested window.")
        findings.append(
            f"End-of-window levels range from {min(float(item['end_level']) for item in per_asset_stats):.2f} ft to "
            f"{max(float(item['end_level']) for item in per_asset_stats):.2f} ft."
        )

        if low_assets:
            preview = ", ".join(
                f"{str(item['asset_name'])} ({float(item['end_level']):.2f} ft)"
                for item in low_assets[:5]
            )
            extra = f" +{len(low_assets) - 5} more" if len(low_assets) > 5 else ""
            findings.append(
                f"{len(low_assets)} tank(s) ended at or below {low_level_threshold:.1f} ft: {preview}{extra}."
            )
        else:
            findings.append(f"No tank ended below the {low_level_threshold:.1f} ft threshold.")

        findings.append(
            f"Net movement summary: {len(depleted_assets)} tank(s) depleted, {len(refilled_assets)} tank(s) increased."
        )

        # Keep warnings concise and actionable.
        if low_assets:
            warnings.append(
                f"Low-level risk in {len(low_assets)} tank(s); prioritize refill planning for those assets."
            )
        if missing_capacity_assets:
            warnings.append(
                f"MaxFt missing for {len(missing_capacity_assets)} asset(s), percentage-filled may be incomplete."
            )

        # Include a short sample of per-asset outcomes for LLM grounding.
        compact_asset_lines = []
        for item in per_asset_stats[:10]:
            pct_text = (
                f", {float(item['end_pct']):.1f}%"
                if item["end_pct"] is not None
                else ""
            )
            compact_asset_lines.append(
                f"{item['asset_name']}: start {float(item['start_level']):.2f} ft, "
                f"end {float(item['end_level']):.2f} ft, "
                f"min {float(item['min_level']):.2f} ft, max {float(item['max_level']):.2f} ft{pct_text}."
            )
        findings.extend(compact_asset_lines)

        # Minimal window-level warning summary.
        stale = any(bool(row.get("is_stale")) for row in rows)
        if stale:
            warnings.append("Some tank readings in the window were stale.")

        large_drops = [
            item for item in per_asset_stats
            if float(item["net_change"]) <= -1.5
        ]
        if large_drops:
            warnings.append(f"Sharp depletion detected in {len(large_drops)} tank(s).")

        return {"warnings": warnings, "findings": findings}

    def _as_float(self, value: object) -> float | None:
        if isinstance(value, bool) or value is None:
            return None
        if isinstance(value, (int, float, Decimal)):
            return float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                return float(Decimal(stripped))
            except (InvalidOperation, ValueError):
                return None
        return None

    def _estimate_cooling_cycles(self, temperatures: list[float], threshold: float) -> int:
        cycles = 0
        cooling = False
        for temperature in reversed(temperatures):
            if not cooling and temperature > threshold:
                cooling = True
            elif cooling and temperature <= threshold:
                cycles += 1
                cooling = False
        return cycles

    def _cooling_consumption_summary(self, rows: list[dict]) -> dict[str, list[str]]:
        points: list[tuple[datetime, float]] = []
        for row in rows:
            timestamp = self._parse_datetime(row.get("recorded_at")) or self._parse_datetime(row.get("timestamp"))
            temperature = self._as_float(row.get("temperature"))
            if timestamp is None or temperature is None:
                continue
            points.append((timestamp, temperature))

        if len(points) < 3:
            return {"findings": [], "warnings": []}

        points.sort(key=lambda item: item[0])
        threshold = 20.0
        last_peak_idx = 0
        in_cycle = False
        cycle_start_idx: int | None = None
        valley_idx: int | None = None
        cycles: list[tuple[datetime, datetime]] = []

        for idx in range(1, len(points)):
            prev_temp = points[idx - 1][1]
            current_temp = points[idx][1]

            if not in_cycle:
                if current_temp >= prev_temp:
                    last_peak_idx = idx
                elif current_temp < prev_temp and current_temp < threshold:
                    cycle_start_idx = last_peak_idx
                    valley_idx = idx
                    in_cycle = True
            else:
                if valley_idx is None or current_temp <= points[valley_idx][1]:
                    valley_idx = idx
                if current_temp >= threshold:
                    start_time = points[cycle_start_idx or 0][0]
                    end_time = points[valley_idx or idx][0]
                    cycles.append((start_time, end_time))
                    in_cycle = False
                    cycle_start_idx = None
                    valley_idx = None
                    last_peak_idx = idx

        if in_cycle and cycle_start_idx is not None:
            cycles.append((points[cycle_start_idx][0], points[-1][0]))

        if not cycles:
            return {
                "findings": ["No confirmed cooling cycle reached the ON threshold of 20C in the returned time window."],
                "warnings": [],
            }

        shift_anchor = datetime.combine(points[0][0].date(), time(hour=6), tzinfo=points[0][0].tzinfo)
        total_on_seconds = 0.0
        clipped_cycles: list[tuple[datetime, datetime]] = []
        for start_time, end_time in cycles:
            adjusted_start = max(start_time, shift_anchor)
            if end_time > adjusted_start:
                total_on_seconds += (end_time - adjusted_start).total_seconds()
                clipped_cycles.append((adjusted_start, end_time))

        total_hours = total_on_seconds / 3600
        findings = [
            f"Inferred cooling cycle count for the requested window is {len(clipped_cycles)}.",
            f"Total inferred ON time from temperature-driven cooling cycles is {total_hours:.2f} hours, clipped to the 6:00 AM shift start where needed.",
        ]
        if clipped_cycles and clipped_cycles[-1][1] == points[-1][0]:
            findings.append("The latest cooling cycle appears to still be active, so ON time was counted up to the newest timestamp.")

        return {"findings": findings, "warnings": []}

    def _generic_trend_findings(self, metric_summary: dict[str, dict[str, float]]) -> list[str]:
        findings: list[str] = []
        priority_columns = [
            "temperature",
            "humidity",
            "total_count",
            "current_level_ft",
            "level_change",
        ]
        emitted = 0
        for column in priority_columns + [column for column in metric_summary if column not in priority_columns]:
            summary = metric_summary.get(column)
            if not summary:
                continue
            drift = summary["latest"] - summary["avg"]
            findings.append(
                f"{column} latest value is {summary['latest']:.2f}, with a window average of {summary['avg']:.2f} and range {summary['min']:.2f} to {summary['max']:.2f}."
            )
            if abs(drift) > 0:
                findings.append(
                    f"{column} is currently {self._describe_trend(drift, '', include_magnitude=False)} versus its window average."
                )
            emitted += 1
            if emitted >= 2:
                break
        return findings

    def _describe_trend(
        self,
        delta: float,
        unit: str,
        *,
        precision: int = 2,
        include_magnitude: bool = True,
    ) -> str:
        magnitude = f"{abs(delta):.{precision}f}"
        unit_suffix = f" {unit}".rstrip() if unit else ""
        if abs(delta) < 1e-9:
            return "flat"
        if delta > 0:
            return f"up by {magnitude}{unit_suffix}" if include_magnitude else "above"
        return f"down by {magnitude}{unit_suffix}" if include_magnitude else "below"

    def _parse_datetime(self, value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if not isinstance(value, str):
            return None
        for fmt in ("%d %b %Y, %I:%M:%S %p", "%d %b %Y, %I:%M %p", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _normalize_warn_type(self, value: object) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        mapping = {
            "0x11": "warn",
            "11": "warn",
            "0x12": "mute",
            "12": "mute",
            "0x14": "low-vol",
            "14": "low-vol",
            "0x15": "fault",
            "15": "fault",
            "0x17": "ok",
            "17": "ok",
            "0x1a": "remove",
            "1a": "remove",
            "26": "remove",
            "0x1b": "install",
            "1b": "install",
            "27": "install",
            "0x1e": "ok-vol-test",
            "1e": "ok-vol-test",
            "30": "ok-vol-test",
            "0x1f": "low-vol-test",
            "1f": "low-vol-test",
            "31": "low-vol-test",
            "1": "warn",
            "true": "warn",
        }
        return mapping.get(text, text)


deterministic_analysis_service = DeterministicAnalysisService()
