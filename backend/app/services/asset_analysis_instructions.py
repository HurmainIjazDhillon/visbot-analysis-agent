from __future__ import annotations

from app.core.config import get_settings
from app.models.schemas import AssetProfile


ASSET_TYPE_ANALYSIS_GUIDANCE = {
    "cold_room": """
For cold rooms, prioritize:
- live device health and payload freshness
- current temperature and humidity
- acceptable temperature band compliance
- historical drift, highs, lows, and instability
- cooling cycle estimation and repeated threshold breaches
- consumption analysis from temperature history when the asset belongs to T & H Monitoring

Preferred analysis choice:
- if user asks about live/current/status -> live_status
- if the asset is in T & H Monitoring and the user asks for a time window, prefer consumption_analysis
- if user asks about temperature pattern, cooling, stability -> cooling_cycle_analysis
- if user asks for previous day/week/month comparison -> historical_variance
- if user asks about abnormal behavior -> anomaly_detection
""".strip(),
    "filling_machine": """
For filling machines, prioritize:
- current production counters
- per-shift throughput
- stale payload detection
- production movement over the requested time window
- identifying flat output, drops, or sudden spikes

Preferred analysis choice:
- if user asks about live/current/status -> live_status
- if user asks about production, throughput, utilization -> throughput_utilization
- if user asks about previous windows or comparisons -> historical_variance
- if user asks about unusual behavior -> anomaly_detection
""".strip(),
    "tank": """
For tanks, prioritize:
- current level
- short-term and long-term rate of change
- refill and depletion pattern
- sharp drops and low-level risk
- stale payload detection

Preferred analysis choice:
- if user asks about live/current/status -> live_status
- if user asks about level movement or stock behavior -> inventory_level_analysis
- if user asks for historical movement -> trend_analysis
- if user asks about unusual drops -> anomaly_detection
""".strip(),
    "smoke_alarm": """
For smoke alarm systems, prioritize:
- Enlisting ALL assets and ALL their attributes in a concise summary.
- Use the 'static_attributes' JSON provided in the data rows to discover all configured attributes for an asset, even if they have no live telemetry.
- If all are okay, keep the per-asset summary brief (one line per asset).
- If smoke, fire, fault, or low-battery is detected, provide the detailed breakdown immediately for that specific asset.
- Highlight smoke/fire detection events prominently.
- Ensure health status, battery, temperature, and humidity are mentioned for every device.

Preferred analysis choice:
- if user asks about live/current/status -> live_status
- if user asks whether smoke/fire happened -> anomaly_detection
- if user asks about a category/group -> group_status
""".strip(),
    "energy_meter": """
For energy meters, prioritize:
- overall power and energy consumption
- peak usage identification
- breaking down consumption by child meters
- power factor anomalies
- voltage or current imbalances

Preferred analysis choice:
- if user asks about live/current/status -> live_status
- if user asks about consumption, peak usage, or breakdown -> consumption_analysis
- if user asks about historical trends -> trend_analysis
- if user asks about a category/group -> group_status
""".strip(),
}


ASSET_NAME_OVERRIDES = {
    "Cold Room 1": """
This cold room should be treated as a temperature-preservation asset.
The final answer should clearly mention whether it is stable, outside band,
or stale/offline, and whether temperature movement suggests a cooling issue.
""".strip(),
    "Filling Machine 1": """
This machine should be treated as a throughput asset.
The final answer should clearly mention total count movement, strongest shift,
and whether the payload looks stale or flat.
""".strip(),
    "Oil Tank 1": """
This tank should be treated as an inventory asset.
The final answer should clearly mention current level, whether the trend is falling,
and whether the level looks risky.
""".strip(),
}


def build_asset_llm_context(asset: AssetProfile) -> str:
    parts = [
        f"Asset selected from chat: {asset.name}",
        f"Asset type: {asset.asset_type}",
        f"Is device with sensor data: {asset.is_device}",
        f"Parent asset: {asset.parent_name or 'None'}",
        f"Direct child count: {asset.child_count}",
        f"Asset description: {asset.description}",
        f"Lookup aliases: {', '.join(asset.lookup_names) if asset.lookup_names else asset.name}",
        f"Supported analyses: {', '.join(asset.supported_analyses)}",
        f"Important metrics: {', '.join(asset.metrics)}",
        f"Attribute keys: {', '.join(asset.attribute_keys[:20]) if asset.attribute_keys else 'None'}",
        (
            "Exact telemetry keys: "
            f"temperature={asset.temperature_keys}, "
            f"humidity={asset.humidity_keys}, "
            f"level={asset.level_keys}, "
            f"production={asset.production_keys}"
        ),
        (
            "OpenRemote SQL facts: "
            "join openremote.asset_datapoint to openremote.asset using ad.entity_id = a.id, "
            "telemetry is usually under ad.value JSON, "
            "multiple attributes can contain JSON sensor payloads, "
            "the analysis should inspect all relevant JSON-carrying attributes unless the user narrows the request, "
            "historical data should be read from asset datapoints, "
            f"and OpenRemote time should be adjusted to local time by adding the {get_settings().local_time_offset_hours}-hour offset (e.g. ad.timestamp + INTERVAL '{get_settings().local_time_offset_hours} hours')."
        ),
        (
            "Device vs category rule: only assets whose attributes contain keys other than notes and location "
            "should be treated as real sensor devices. Parent/group assets should be analyzed via their child assets."
        ),
        (
            f"Nominal thresholds: {asset.nominal_range if asset.nominal_range else {'min_temperature': 5.0, 'max_temperature': 45.0}}. "
            "Use these thresholds for compliance checks and avoid introducing any unstated default range."
        ),
    ]

    if asset.asset_type == "tank":
        parts.append(
            "Tank rule: for tank-related devices, the actual physical liquid height is MaxFt - TankOilLevelInFeet001. "
            "These values can come from two different attributes, so the analysis must combine them before drawing conclusions. "
            "The final analysis should prioritize showing the percentage filled (e.g., 75% filled) rather than just the height, "
            "using the formula (liquid height / MaxFt) * 100. "
            "Do not explain how you calculated the level or percentage to the user."
        )

    if "t & h monitoring" in f"{asset.name} {asset.parent_name}".lower():
        parts.append(
            "T & H Monitoring consumption rule: determine cold-room ON/OFF state from temperature trend rather than a direct signal. "
            "A cooling cycle starts at the last peak before temperature begins decreasing and later drops below 20C. "
            "The cycle stays ON while temperature remains below 20C and ends at the valley before temperature rises back to 20C or above."
            "Sum all such cycle durations across the requested window, clip any cycle that began before the shift to 6:00 AM, "
            "and if a cycle is still active at the end of the data, count it up to the latest timestamp."
        )

    if asset.asset_type == "smoke_alarm":
        parts.append(
            "Smoke-alarm rule: inspect every relevant JSON-carrying attribute for each end device. "
            "Treat warn='warn' as smoke/fire detected, warn='mute' as alarm was muted after detection, "
            "warn='fault' as device fault, warn='remove' as device removed, warn='install' as device installed, "
            "warn='low-vol' or warn='low-vol-test' as low battery/voltage, and warn='ok' as normal. "
            "The answer must clearly state whether smoke/fire was detected and include the timestamps of those events."
        )

    type_guidance = ASSET_TYPE_ANALYSIS_GUIDANCE.get(asset.asset_type)
    if type_guidance:
        parts.append(type_guidance)

    override = ASSET_NAME_OVERRIDES.get(asset.name)
    if override:
        parts.append(override)

    if asset.analysis_instructions:
        parts.append(f"Asset-specific rules: {asset.analysis_instructions}")

    return "\n\n".join(parts)
