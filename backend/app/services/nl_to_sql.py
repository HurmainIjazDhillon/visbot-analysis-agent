from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.core.config import get_settings
from app.models.schemas import AssetProfile, TimeWindow
from app.services.sql_guard import validate_read_only_sql


@dataclass
class SQLPlan:
    query: str
    explanation: str


class NLToSQLService:
    def __init__(self) -> None:
        self._settings = get_settings()

    def build_query(
        self,
        asset: AssetProfile,
        question: str,
        analysis_name: str,
        time_window: TimeWindow,
    ) -> SQLPlan:
        query = self._build_asset_query(asset=asset, analysis_name=analysis_name, time_window=time_window)
        validate_read_only_sql(query)

        explanation = (
            f"Generated a read-only OpenRemote query for `{asset.name}` using "
            f"`{analysis_name}` over {time_window.label}."
        )
        return SQLPlan(query=query, explanation=explanation)

    def normalize_generated_query(self, query: str, asset: AssetProfile) -> str:
        normalized = query
        normalized = normalized.replace("ad.asset_id", "ad.entity_id")
        normalized = normalized.replace("ad.AssetId", "ad.entity_id")
        normalized = normalized.replace("asset_datapoint.asset_id", "asset_datapoint.entity_id")

        if "JOIN openremote.asset_datapoint ad ON ad.entity_id = a.id" not in normalized:
            normalized = normalized.replace(
                "JOIN openremote.asset_datapoint ad ON a.id = ad.entity_id",
                "JOIN openremote.asset_datapoint ad ON ad.entity_id = a.id",
            )

        stripped = normalized.lstrip()
        if "descendants AS" in normalized and stripped.startswith("WITH ") and not stripped.startswith("WITH RECURSIVE "):
            normalized = normalized.replace("WITH ", "WITH RECURSIVE ", 1)

        return normalized

    def _build_asset_query(self, asset: AssetProfile, analysis_name: str, time_window: TimeWindow) -> str:
        if not asset.is_device and asset.child_count > 0:
            if asset.asset_type == "tank":
                return self._build_tank_group_query(asset, time_window)
            if asset.asset_type == "smoke_alarm":
                return self._build_smoke_group_query(asset, time_window)
            if asset.asset_type == "energy_meter":
                return self._build_energy_group_query(asset, time_window)
            if asset.asset_type == "aqi":
                return self._build_aqi_group_query(asset, time_window)
            return self._build_group_query(asset, time_window)
        if asset.asset_type == "cold_room":
            return self._build_cold_room_query(asset, time_window)
        if asset.asset_type == "filling_machine":
            return self._build_filling_machine_query(asset, time_window)
        if asset.asset_type == "tank":
            return self._build_tank_query(asset, time_window)
        if asset.asset_type == "smoke_alarm":
            return self._build_smoke_device_query(asset, time_window)
        return self._build_generic_query(asset, time_window)

    def _build_smoke_group_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        root_id = self._escape_sql(asset.db_asset_id or asset.asset_id)
        offset = self._settings.local_time_offset_hours
        timestamp_window_filter = self._window_filter_sql("ad.timestamp", time_window, local=False)
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes, attributes::jsonb AS static_attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes, c.attributes::jsonb AS static_attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name, static_attributes
    FROM hierarchy
    WHERE id != '{root_id}'
),
raw_points AS (
    SELECT
        d.name AS asset_name,
        d.static_attributes,
        ad.attribute_name,
        (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
        ad.value AS sensor_data,
        LOWER(COALESCE(ad.value->>'warn', CASE WHEN ad.attribute_name ILIKE '%warn%' OR ad.attribute_name ILIKE '%smoke%' THEN ad.value::text END, '')) AS warn_raw,
        NULLIF(regexp_replace(COALESCE(ad.value->>'temp', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS temperature,
        NULLIF(regexp_replace(COALESCE(ad.value->>'humi', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS humidity,
        NULLIF(regexp_replace(COALESCE(ad.value->>'bat_percent', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS bat_percent,
        NULLIF(regexp_replace(COALESCE(ad.value->>'bat_voltage', CASE WHEN ad.attribute_name ILIKE '%voltage%' THEN ad.value::text END, ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS bat_voltage,
        CASE 
            WHEN LOWER(COALESCE(ad.value->>'warn', CASE WHEN ad.attribute_name ILIKE '%warn%' OR ad.attribute_name ILIKE '%smoke%' THEN ad.value::text END, '')) IN ('warn', 'mute', '11', '0x11', '12', '0x12', '1', 'true', 'on', 'alarm') THEN 1
            ELSE 0 
        END AS smoke_detected,
        COALESCE(ad.value->>'name', ad.attribute_name) AS sensor_name,
        date_trunc('day', ad.timestamp + INTERVAL '{offset} hours') AS day_bucket
    FROM devices d
    LEFT JOIN {self._settings.openremote_schema}.asset_datapoint ad ON ad.entity_id = d.id
    WHERE 1=1
      {timestamp_window_filter}
),
bucketed AS (
    SELECT
        rp.*,
        ntile({per_day_points}) OVER (
            PARTITION BY rp.asset_name, rp.attribute_name, rp.day_bucket
            ORDER BY rp.timestamp
        ) AS sample_bucket
    FROM raw_points rp
),
sampled_points AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_name, b.attribute_name, b.day_bucket, b.sample_bucket
                ORDER BY b.timestamp DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
)
SELECT
    asset_name,
    static_attributes,
    attribute_name,
    timestamp AS recorded_at,
    sensor_name,
    CASE
        WHEN warn_raw IN ('warn', 'mute', 'low-vol', 'fault', 'ok', 'remove', 'install', 'ok-vol-test', 'low-vol-test') THEN warn_raw
        WHEN warn_raw IN ('17', '0x17') THEN 'ok'
        WHEN warn_raw IN ('11', '0x11') THEN 'warn'
        WHEN warn_raw IN ('12', '0x12') THEN 'mute'
        WHEN warn_raw IN ('14', '0x14') THEN 'low-vol'
        WHEN warn_raw IN ('15', '0x15') THEN 'fault'
        WHEN warn_raw IN ('1a', '0x1a', '26') THEN 'remove'
        WHEN warn_raw IN ('1b', '0x1b', '27') THEN 'install'
        WHEN warn_raw IN ('1e', '0x1e', '30') THEN 'ok-vol-test'
        WHEN warn_raw IN ('1f', '0x1f', '31') THEN 'low-vol-test'
        WHEN warn_raw = '' THEN NULL
        ELSE 'unknown'
    END AS warn_type,
    temperature,
    humidity,
    bat_percent,
    bat_voltage,
    smoke_detected,
    sensor_data
FROM sampled_points
ORDER BY asset_name, recorded_at ASC, attribute_name;
""".strip()
        return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes, attributes::jsonb AS static_attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes, c.attributes::jsonb AS static_attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name, static_attributes
    FROM hierarchy
    WHERE id != '{root_id}'
),
raw_points AS (
    SELECT
        d.name AS asset_name,
        d.static_attributes,
        ad.attribute_name,
        (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
        ad.value AS sensor_data,
        LOWER(COALESCE(ad.value->>'warn', '')) AS warn_raw,
        NULLIF(regexp_replace(COALESCE(ad.value->>'temp', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS temperature,
        NULLIF(regexp_replace(COALESCE(ad.value->>'humi', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS humidity,
        NULLIF(regexp_replace(COALESCE(ad.value->>'bat_percent', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS bat_percent,
        NULLIF(regexp_replace(COALESCE(ad.value->>'bat_voltage', CASE WHEN ad.attribute_name ILIKE '%voltage%' THEN ad.value::text END, ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS bat_voltage,
        CASE 
            WHEN LOWER(COALESCE(ad.value->>'warn', CASE WHEN ad.attribute_name ILIKE '%warn%' OR ad.attribute_name ILIKE '%smoke%' THEN ad.value::text END, '')) IN ('warn', 'mute', '11', '0x11', '12', '0x12', '1', 'true', 'on', 'alarm') THEN 1
            ELSE 0 
        END AS smoke_detected,
        COALESCE(ad.value->>'name', ad.attribute_name) AS sensor_name
    FROM devices d
    LEFT JOIN {self._settings.openremote_schema}.asset_datapoint ad ON ad.entity_id = d.id
    WHERE 1=1
      {timestamp_window_filter}
)
SELECT
    asset_name,
    static_attributes,
    attribute_name,
    timestamp AS recorded_at,
    sensor_name,
    CASE
        WHEN warn_raw IN ('warn', 'mute', 'low-vol', 'fault', 'ok', 'remove', 'install', 'ok-vol-test', 'low-vol-test') THEN warn_raw
        WHEN warn_raw IN ('17', '0x17') THEN 'ok'
        WHEN warn_raw IN ('11', '0x11') THEN 'warn'
        WHEN warn_raw IN ('12', '0x12') THEN 'mute'
        WHEN warn_raw IN ('14', '0x14') THEN 'low-vol'
        WHEN warn_raw IN ('15', '0x15') THEN 'fault'
        WHEN warn_raw IN ('1a', '0x1a', '26') THEN 'remove'
        WHEN warn_raw IN ('1b', '0x1b', '27') THEN 'install'
        WHEN warn_raw IN ('1e', '0x1e', '30') THEN 'ok-vol-test'
        WHEN warn_raw IN ('1f', '0x1f', '31') THEN 'low-vol-test'
        WHEN warn_raw = '' THEN NULL
        ELSE 'unknown'
    END AS warn_type,
    temperature,
    humidity,
    bat_percent,
    bat_voltage,
    smoke_detected,
    sensor_data
FROM raw_points
ORDER BY asset_name, recorded_at ASC, attribute_name;
""".strip()

    def _build_smoke_device_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        exact_match = self._exact_asset_match_sql(asset.name)
        offset = self._settings.local_time_offset_hours
        timestamp_window_filter = self._window_filter_sql("ad.timestamp", time_window, local=False)
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH raw_points AS (
    SELECT
        a.id::text AS asset_id,
        a.name AS asset_name,
        a.attributes::jsonb AS static_attributes,
        ad.attribute_name,
        (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
        ad.value AS sensor_data,
        LOWER(COALESCE(ad.value->>'warn', '')) AS warn_raw,
        NULLIF(regexp_replace(COALESCE(ad.value->>'temp', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS temperature,
        NULLIF(regexp_replace(COALESCE(ad.value->>'humi', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS humidity,
        NULLIF(regexp_replace(COALESCE(ad.value->>'bat_percent', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS bat_percent,
        NULLIF(regexp_replace(COALESCE(ad.value->>'bat_voltage', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS bat_voltage,
        CASE 
            WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('warn', 'mute', '11', '0x11', '12', '0x12', '1', 'true') THEN 1
            ELSE 0 
        END AS smoke_detected,
        COALESCE(ad.value->>'name', ad.attribute_name) AS sensor_name,
        date_trunc('day', ad.timestamp + INTERVAL '{offset} hours') AS day_bucket
    FROM {self._settings.openremote_schema}.asset a
    JOIN {self._settings.openremote_schema}.asset_datapoint ad ON ad.entity_id = a.id
    WHERE {exact_match}
      {timestamp_window_filter}
),
bucketed AS (
    SELECT
        rp.*,
        ntile({per_day_points}) OVER (
            PARTITION BY rp.asset_id, rp.attribute_name, rp.day_bucket
            ORDER BY rp.timestamp
        ) AS sample_bucket
    FROM raw_points rp
),
sampled_points AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_id, b.attribute_name, b.day_bucket, b.sample_bucket
                ORDER BY b.timestamp DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
)
SELECT
    asset_id,
    asset_name,
    static_attributes,
    attribute_name,
    timestamp AS recorded_at,
    sensor_name,
    CASE
        WHEN warn_raw IN ('warn', 'mute', 'low-vol', 'fault', 'ok', 'remove', 'install', 'ok-vol-test', 'low-vol-test') THEN warn_raw
        WHEN warn_raw IN ('17', '0x17') THEN 'ok'
        WHEN warn_raw IN ('11', '0x11') THEN 'warn'
        WHEN warn_raw IN ('12', '0x12') THEN 'mute'
        WHEN warn_raw IN ('14', '0x14') THEN 'low-vol'
        WHEN warn_raw IN ('15', '0x15') THEN 'fault'
        WHEN warn_raw IN ('1a', '0x1a', '26') THEN 'remove'
        WHEN warn_raw IN ('1b', '0x1b', '27') THEN 'install'
        WHEN warn_raw IN ('1e', '0x1e', '30') THEN 'ok-vol-test'
        WHEN warn_raw IN ('1f', '0x1f', '31') THEN 'low-vol-test'
        WHEN warn_raw = '' THEN NULL
        ELSE 'unknown'
    END AS warn_type,
    temperature,
    humidity,
    bat_percent,
    bat_voltage,
    smoke_detected,
    sensor_data
FROM sampled_points
ORDER BY recorded_at ASC, attribute_name;
""".strip()
        return f"""
SELECT
    a.id::text AS asset_id,
    a.name AS asset_name,
    a.attributes::jsonb AS static_attributes,
    ad.attribute_name,
    (ad.timestamp + INTERVAL '{offset} hours') AS recorded_at,
    COALESCE(ad.value->>'name', ad.attribute_name) AS sensor_name,
    CASE
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('warn', 'mute', 'low-vol', 'fault', 'ok', 'remove', 'install', 'ok-vol-test', 'low-vol-test') THEN LOWER(ad.value->>'warn')
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('17', '0x17') THEN 'ok'
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('11', '0x11') THEN 'warn'
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('12', '0x12') THEN 'mute'
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('14', '0x14') THEN 'low-vol'
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('15', '0x15') THEN 'fault'
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('1a', '0x1a', '26') THEN 'remove'
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('1b', '0x1b', '27') THEN 'install'
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('1e', '0x1e', '30') THEN 'ok-vol-test'
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('1f', '0x1f', '31') THEN 'low-vol-test'
        WHEN COALESCE(ad.value->>'warn', '') = '' THEN NULL
        ELSE 'unknown'
    END AS warn_type,
    NULLIF(regexp_replace(COALESCE(ad.value->>'temp', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS temperature,
    NULLIF(regexp_replace(COALESCE(ad.value->>'humi', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS humidity,
    NULLIF(regexp_replace(COALESCE(ad.value->>'bat_percent', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS bat_percent,
    NULLIF(regexp_replace(COALESCE(ad.value->>'bat_voltage', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS bat_voltage,
    CASE 
        WHEN LOWER(COALESCE(ad.value->>'warn', '')) IN ('warn', 'mute', '11', '0x11', '12', '0x12', '1', 'true') THEN 1
        ELSE 0 
    END AS smoke_detected,
    (ad.timestamp + INTERVAL '{offset} hours') AS recorded_at,
    ad.value AS sensor_data
FROM {self._settings.openremote_schema}.asset a
JOIN {self._settings.openremote_schema}.asset_datapoint ad ON ad.entity_id = a.id
WHERE {exact_match}
  {timestamp_window_filter}
ORDER BY recorded_at ASC, ad.attribute_name;
""".strip()

    def _build_group_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        interval = self._interval_sql(time_window)
        root_id = self._escape_sql(asset.db_asset_id or asset.asset_id)
        offset = self._settings.local_time_offset_hours
        timestamp_window_filter = self._window_filter_sql("ad.timestamp", time_window, local=False)
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name
    FROM hierarchy
    WHERE id != '{root_id}'
      AND attributes IS NOT NULL
      AND (attributes - 'notes' - 'location') != '{{}}'::jsonb
),
raw_points AS (
    SELECT
        ad.entity_id,
        ad.attribute_name,
        (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
        ad.value,
        date_trunc('day', ad.timestamp + INTERVAL '{offset} hours') AS day_bucket
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN devices d ON d.id = ad.entity_id
    WHERE 1=1
      {timestamp_window_filter}
),
bucketed AS (
    SELECT
        rp.*,
        ntile({per_day_points}) OVER (
            PARTITION BY rp.entity_id, rp.day_bucket
            ORDER BY rp.timestamp
        ) AS sample_bucket
    FROM raw_points rp
),
sampled_points AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.entity_id, b.day_bucket, b.sample_bucket
                ORDER BY b.timestamp DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
)
SELECT
    d.name AS asset_name,
    sp.attribute_name,
    sp.timestamp,
    sp.value AS sensor_data
FROM devices d
LEFT JOIN sampled_points sp ON sp.entity_id = d.id
ORDER BY d.name, sp.timestamp, sp.attribute_name;
""".strip()
        return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name
    FROM hierarchy
    WHERE id != '{root_id}'
      AND attributes IS NOT NULL
      AND (attributes - 'notes' - 'location') != '{{}}'::jsonb
),
latest_points AS (
    SELECT DISTINCT ON (ad.entity_id, ad.attribute_name)
        ad.entity_id,
        ad.attribute_name,
        (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
        ad.value
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN devices d ON d.id = ad.entity_id
    WHERE 1=1
      {timestamp_window_filter}
    ORDER BY ad.entity_id, ad.attribute_name, ad.timestamp DESC
)
SELECT
    d.name AS asset_name,
    lp.attribute_name,
    lp.timestamp,
    lp.value AS sensor_data
FROM devices d
LEFT JOIN latest_points lp ON lp.entity_id = d.id
ORDER BY d.name, lp.attribute_name;
""".strip()

    def _build_energy_group_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        root_id = self._escape_sql(asset.db_asset_id or asset.asset_id)
        offset = self._settings.local_time_offset_hours
        local_time_window_filter = self._window_filter_sql(
            f"(ad.timestamp + INTERVAL '{offset} hours')",
            time_window,
            local=True,
        )
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name
    FROM hierarchy
    WHERE id != '{root_id}'
      AND attributes IS NOT NULL
      AND (attributes - 'notes' - 'location') != '{{}}'::jsonb
),
raw_points AS (
    SELECT
        d.name AS asset_name,
        ad.attribute_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        ad.value AS sensor_data,
        NULLIF(regexp_replace(COALESCE(ad.value->>'CurrentA', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS current_a,
        NULLIF(regexp_replace(COALESCE(ad.value->>'CurrentB', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS current_b,
        NULLIF(regexp_replace(COALESCE(ad.value->>'CurrentC', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS current_c,
        NULLIF(regexp_replace(COALESCE(ad.value->>'PowerA', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS power_a,
        NULLIF(regexp_replace(COALESCE(ad.value->>'PowerB', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS power_b,
        NULLIF(regexp_replace(COALESCE(ad.value->>'PowerC', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS power_c,
        NULLIF(regexp_replace(COALESCE(ad.value->>'EnergyTotalABC', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS energy_total,
        date_trunc('day', ad.timestamp + INTERVAL '{offset} hours') AS day_bucket
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN devices d ON d.id = ad.entity_id
    WHERE 1=1
      {local_time_window_filter}
),
bucketed AS (
    SELECT
        rp.*,
        ntile({per_day_points}) OVER (
            PARTITION BY rp.asset_name, rp.attribute_name, rp.day_bucket
            ORDER BY rp.timestamp
        ) AS sample_bucket
    FROM raw_points rp
),
sampled_points AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_name, b.attribute_name, b.day_bucket, b.sample_bucket
                ORDER BY b.local_time DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
)
SELECT
    asset_name,
    attribute_name,
    local_time AS recorded_at,
    current_a,
    current_b,
    current_c,
    power_a,
    power_b,
    power_c,
    energy_total,
    sensor_data
FROM sampled_points
ORDER BY asset_name, recorded_at ASC, attribute_name;
""".strip()
        return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name
    FROM hierarchy
    WHERE id != '{root_id}'
      AND attributes IS NOT NULL
      AND (attributes - 'notes' - 'location') != '{{}}'::jsonb
),
raw_points AS (
    SELECT
        d.name AS asset_name,
        ad.attribute_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        ad.value AS sensor_data,
        NULLIF(regexp_replace(COALESCE(ad.value->>'CurrentA', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS current_a,
        NULLIF(regexp_replace(COALESCE(ad.value->>'CurrentB', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS current_b,
        NULLIF(regexp_replace(COALESCE(ad.value->>'CurrentC', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS current_c,
        NULLIF(regexp_replace(COALESCE(ad.value->>'PowerA', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS power_a,
        NULLIF(regexp_replace(COALESCE(ad.value->>'PowerB', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS power_b,
        NULLIF(regexp_replace(COALESCE(ad.value->>'PowerC', CASE WHEN ad.attribute_name ILIKE '%PowerC%' THEN ad.value::text END, ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS power_c,
        NULLIF(regexp_replace(COALESCE(ad.value->>'EnergyTotalABC', CASE WHEN ad.attribute_name ILIKE '%Energy%' THEN ad.value::text END, ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS energy_total
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN devices d ON d.id = ad.entity_id
    WHERE 1=1
      {local_time_window_filter}
)
SELECT
    asset_name,
    attribute_name,
    local_time AS recorded_at,
    current_a,
    current_b,
    current_c,
    power_a,
    power_b,
    power_c,
    energy_total,
    sensor_data
FROM raw_points
ORDER BY asset_name, recorded_at ASC, attribute_name;
""".strip()

    def _build_aqi_group_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        root_id = self._escape_sql(asset.db_asset_id or asset.asset_id)
        offset = self._settings.local_time_offset_hours
        local_time_window_filter = self._window_filter_sql(
            f"(ad.timestamp + INTERVAL '{offset} hours')",
            time_window,
            local=True,
        )
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name
    FROM hierarchy
    WHERE id != '{root_id}'
      AND attributes IS NOT NULL
      AND (attributes - 'notes' - 'location') != '{{}}'::jsonb
),
raw_points AS (
    SELECT
        d.name AS asset_name,
        ad.attribute_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        ad.value AS sensor_data,
        NULLIF(regexp_replace(COALESCE(ad.value->>'AQIIndex001', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS aqi_index,
        NULLIF(regexp_replace(COALESCE(ad.value->>'pm25', ad.value->>'PM25', ad.value->>'PM2.5', ad.value->>'pm2.5', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS pm25,
        NULLIF(regexp_replace(COALESCE(ad.value->>'pm10', ad.value->>'PM10', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS pm10,
        NULLIF(regexp_replace(COALESCE(ad.value->>'co2', ad.value->>'CO2', ad.value->>'AQICO2001', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS co2,
        NULLIF(regexp_replace(COALESCE(ad.value->>'voc', ad.value->>'VOC', ad.value->>'AQIVOC001', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS voc,
        date_trunc('day', ad.timestamp + INTERVAL '{offset} hours') AS day_bucket
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN devices d ON d.id = ad.entity_id
    WHERE 1=1
      {local_time_window_filter}
),
bucketed AS (
    SELECT
        rp.*,
        ntile({per_day_points}) OVER (
            PARTITION BY rp.asset_name, rp.attribute_name, rp.day_bucket
            ORDER BY rp.raw_time
        ) AS sample_bucket
    FROM raw_points rp
),
sampled_points AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_name, b.attribute_name, b.day_bucket, b.sample_bucket
                ORDER BY b.local_time DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
)
SELECT
    asset_name,
    attribute_name,
    local_time AS recorded_at,
    aqi_index,
    pm25,
    pm10,
    co2,
    voc,
    sensor_data
FROM sampled_points
ORDER BY asset_name, recorded_at ASC, attribute_name;
""".strip()
        return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name
    FROM hierarchy
    WHERE id != '{root_id}'
      AND attributes IS NOT NULL
      AND (attributes - 'notes' - 'location') != '{{}}'::jsonb
),
raw_points AS (
    SELECT
        d.name AS asset_name,
        ad.attribute_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        ad.value AS sensor_data,
        NULLIF(regexp_replace(COALESCE(ad.value->>'AQIIndex001', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS aqi_index,
        NULLIF(regexp_replace(COALESCE(ad.value->>'pm25', ad.value->>'PM25', ad.value->>'PM2.5', ad.value->>'pm2.5', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS pm25,
        NULLIF(regexp_replace(COALESCE(ad.value->>'pm10', ad.value->>'PM10', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS pm10,
        NULLIF(regexp_replace(COALESCE(ad.value->>'co2', ad.value->>'CO2', ad.value->>'AQICO2001', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS co2,
        NULLIF(regexp_replace(COALESCE(ad.value->>'voc', ad.value->>'VOC', ad.value->>'AQIVOC001', ''), '[^0-9\\.-]', '', 'g'), '')::numeric AS voc
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN devices d ON d.id = ad.entity_id
    WHERE 1=1
      {local_time_window_filter}
)
SELECT
    asset_name,
    attribute_name,
    local_time AS recorded_at,
    aqi_index,
    pm25,
    pm10,
    co2,
    voc,
    sensor_data
FROM raw_points
ORDER BY asset_name, recorded_at ASC, attribute_name;
""".strip()

    def _build_tank_group_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        interval = self._interval_sql(time_window)
        root_id = self._escape_sql(asset.db_asset_id or asset.asset_id)
        offset = self._settings.local_time_offset_hours
        timestamp_window_filter = self._window_filter_sql("ad.timestamp", time_window, local=False)
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name, attributes
    FROM hierarchy
    WHERE id != '{root_id}'
      AND attributes IS NOT NULL
      AND (attributes - 'notes' - 'location') != '{{}}'::jsonb
),
raw_level_points AS (
    SELECT
        ad.entity_id,
        ad.attribute_name,
        (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
        ad.value,
        date_trunc('day', ad.timestamp + INTERVAL '{offset} hours') AS day_bucket
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN devices d ON d.id = ad.entity_id
    WHERE 1=1
      AND (
        NULLIF(ad.value->>'TankOilLevelInFeet001', '') IS NOT NULL
        OR NULLIF(ad.value->>'Level', '') IS NOT NULL
      )
      {timestamp_window_filter}
),
bucketed AS (
    SELECT
        rp.*,
        ntile({per_day_points}) OVER (
            PARTITION BY rp.entity_id, rp.day_bucket
            ORDER BY rp.timestamp
        ) AS sample_bucket
    FROM raw_level_points rp
),
tank_payload AS (
    SELECT *
    FROM (
        SELECT
            b.entity_id,
            b.attribute_name,
            b.timestamp,
            b.value,
            row_number() OVER (
                PARTITION BY b.entity_id, b.day_bucket, b.sample_bucket
                ORDER BY b.timestamp DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
),
capacity_payload AS (
    SELECT
        d.id AS entity_id,
        'asset.maxcapacity.value' AS attribute_name,
        NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric AS max_ft_from_payload
    FROM devices d
    WHERE NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '') IS NOT NULL
)
SELECT
    d.name AS asset_name,
    tp.attribute_name AS level_attribute,
    COALESCE(cp.attribute_name, 'asset.maxcapacity.value') AS max_ft_attribute,
    tp.timestamp,
    COALESCE(
        NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric,
        cp.max_ft_from_payload
    ) AS max_ft,
    COALESCE(
        NULLIF(tp.value->>'TankOilLevelInFeet001', '')::numeric,
        NULLIF(tp.value->>'Level', '')::numeric
    ) AS tank_oil_level_in_feet_001,
    GREATEST(
        COALESCE(
            NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric,
            cp.max_ft_from_payload,
            0
        ) - COALESCE(
            NULLIF(tp.value->>'TankOilLevelInFeet001', '')::numeric,
            NULLIF(tp.value->>'Level', '')::numeric,
            0
        ),
        0
    ) AS current_level_ft,
    ROUND(
        (GREATEST(COALESCE(NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric, cp.max_ft_from_payload, 0) - COALESCE(NULLIF(tp.value->>'TankOilLevelInFeet001', '')::numeric, NULLIF(tp.value->>'Level', '')::numeric, 0), 0)
        / NULLIF(COALESCE(NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric, cp.max_ft_from_payload), 0)) * 100,
        1
    ) AS percentage_filled,
    tp.value AS sensor_data
FROM devices d
LEFT JOIN tank_payload tp ON tp.entity_id = d.id
LEFT JOIN capacity_payload cp ON cp.entity_id = d.id
ORDER BY d.name, tp.timestamp;
""".strip()
        return f"""
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset
    WHERE id = '{root_id}'
    UNION ALL
    SELECT c.id, c.name, c.parent_id, c.attributes::jsonb AS attributes
    FROM {self._settings.openremote_schema}.asset c
    JOIN hierarchy h ON c.parent_id = h.id
),
devices AS (
    SELECT id, name, attributes
    FROM hierarchy
    WHERE id != '{root_id}'
      AND attributes IS NOT NULL
      AND (attributes - 'notes' - 'location') != '{{}}'::jsonb
),
latest_level_points AS (
    SELECT DISTINCT ON (ad.entity_id)
        ad.entity_id,
        ad.attribute_name,
        (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
        ad.value
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN devices d ON d.id = ad.entity_id
    WHERE 1=1
      AND (
        NULLIF(ad.value->>'TankOilLevelInFeet001', '') IS NOT NULL
        OR NULLIF(ad.value->>'Level', '') IS NOT NULL
      )
      {timestamp_window_filter}
    ORDER BY ad.entity_id, ad.timestamp DESC, ad.attribute_name
),
tank_payload AS (
    SELECT
        lp.entity_id,
        lp.attribute_name,
        lp.timestamp,
        lp.value
    FROM latest_level_points lp
),
capacity_payload AS (
    SELECT
        d.id AS entity_id,
        'asset.maxcapacity.value' AS attribute_name,
        NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric AS max_ft_from_payload
    FROM devices d
    WHERE NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '') IS NOT NULL
)
SELECT
    d.name AS asset_name,
    tp.attribute_name AS level_attribute,
    COALESCE(cp.attribute_name, 'asset.maxcapacity.value') AS max_ft_attribute,
    tp.timestamp,
    COALESCE(
        NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric,
        cp.max_ft_from_payload
    ) AS max_ft,
    COALESCE(
        NULLIF(tp.value->>'TankOilLevelInFeet001', '')::numeric,
        NULLIF(tp.value->>'Level', '')::numeric
    ) AS tank_oil_level_in_feet_001,
    GREATEST(
        COALESCE(
            NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric,
            cp.max_ft_from_payload,
            0
        ) - COALESCE(
            NULLIF(tp.value->>'TankOilLevelInFeet001', '')::numeric,
            NULLIF(tp.value->>'Level', '')::numeric,
            0
        ),
        0
    ) AS current_level_ft,
    ROUND(
        (GREATEST(COALESCE(NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric, cp.max_ft_from_payload, 0) - COALESCE(NULLIF(tp.value->>'TankOilLevelInFeet001', '')::numeric, NULLIF(tp.value->>'Level', '')::numeric, 0), 0) 
        / NULLIF(COALESCE(NULLIF(d.attributes->'maxcapacity'->'value'->>'MaxFt', '')::numeric, cp.max_ft_from_payload), 0)) * 100, 
        1
    ) AS percentage_filled,
    tp.value AS sensor_data
FROM devices d
LEFT JOIN tank_payload tp ON tp.entity_id = d.id
LEFT JOIN capacity_payload cp ON cp.entity_id = d.id
ORDER BY d.name;
""".strip()

    def _build_cold_room_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        temp_expr = self._coalesce_json_value(asset.temperature_keys or ["Temperature", "temperature"])
        humidity_expr = self._coalesce_json_value(asset.humidity_keys or ["Humidity", "humidity"])
        offset = self._settings.local_time_offset_hours
        interval = self._interval_sql(time_window)
        exact_match = self._exact_asset_match_sql(asset.name)
        local_time_window_filter = self._window_filter_sql(
            f"(ad.timestamp + INTERVAL '{offset} hours')",
            time_window,
            local=True,
        )
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH raw_data AS (
    SELECT
        a.id AS asset_uuid,
        a.name AS asset_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        regexp_replace({temp_expr}, '[^0-9\\.-]', '', 'g')::numeric AS temperature,
        regexp_replace({humidity_expr}, '[^0-9\\.-]', '', 'g')::numeric AS humidity
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN {self._settings.openremote_schema}.asset a ON a.id = ad.entity_id
    WHERE {exact_match}
      AND {temp_expr} IS NOT NULL
      AND NULLIF(regexp_replace({temp_expr}, '[^0-9\\.-]', '', 'g'), '') IS NOT NULL
      {local_time_window_filter}
),
bucketed AS (
    SELECT
        rd.*,
        date_trunc('day', rd.local_time) AS day_bucket,
        ntile({per_day_points}) OVER (
            PARTITION BY rd.asset_uuid, date_trunc('day', rd.local_time)
            ORDER BY rd.local_time
        ) AS sample_bucket
    FROM raw_data rd
),
sampled_data AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_uuid, b.day_bucket, b.sample_bucket
                ORDER BY b.local_time DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
)
SELECT
    asset_uuid::text AS asset_id,
    asset_name,
    local_time AS recorded_at,
    temperature,
    humidity,
    raw_time < NOW() - INTERVAL '{asset.nominal_range.get("max_stale_hours", 2)} hours' AS is_stale
FROM sampled_data
ORDER BY recorded_at ASC;
""".strip()

        return f"""
WITH raw_data AS (
    SELECT
        a.id AS asset_uuid,
        a.name AS asset_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        regexp_replace({temp_expr}, '[^0-9\\.-]', '', 'g')::numeric AS temperature,
        regexp_replace({humidity_expr}, '[^0-9\\.-]', '', 'g')::numeric AS humidity
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN {self._settings.openremote_schema}.asset a ON a.id = ad.entity_id
    WHERE {exact_match}
      AND {temp_expr} IS NOT NULL
      AND NULLIF(regexp_replace({temp_expr}, '[^0-9\\.-]', '', 'g'), '') IS NOT NULL
      {local_time_window_filter}
)
SELECT
    asset_uuid::text AS asset_id,
    asset_name,
    local_time AS recorded_at,
    temperature,
    humidity,
    raw_time < NOW() - INTERVAL '{asset.nominal_range.get("max_stale_hours", 2)} hours' AS is_stale
FROM raw_data
ORDER BY recorded_at DESC
LIMIT 500;
""".strip()

    def _build_filling_machine_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        offset = self._settings.local_time_offset_hours
        interval = self._interval_sql(time_window)
        exact_match = self._exact_asset_match_sql(asset.name)
        local_time_window_filter = self._window_filter_sql(
            f"(ad.timestamp + INTERVAL '{offset} hours')",
            time_window,
            local=True,
        )
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH raw_data AS (
    SELECT
        a.id AS asset_uuid,
        a.name AS asset_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        (ad.value->>'prod_shift_1_today')::numeric AS shift_1_count,
        (ad.value->>'prod_shift_2_today')::numeric AS shift_2_count,
        (ad.value->>'prod_shift_3_today')::numeric AS shift_3_count,
        (ad.value->>'prod_total_today')::numeric AS total_count,
        COALESCE(NULLIF(ad.value->>'Timestamp', '')::timestamp, ad.timestamp + INTERVAL '{offset} hours') AS payload_time
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN {self._settings.openremote_schema}.asset a ON a.id = ad.entity_id
    WHERE {exact_match}
      AND (ad.value->>'prod_total_today') IS NOT NULL
      {local_time_window_filter}
),
bucketed AS (
    SELECT
        rd.*,
        date_trunc('day', rd.local_time) AS day_bucket,
        ntile({per_day_points}) OVER (
            PARTITION BY rd.asset_uuid, date_trunc('day', rd.local_time)
            ORDER BY rd.local_time
        ) AS sample_bucket
    FROM raw_data rd
),
sampled_data AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_uuid, b.day_bucket, b.sample_bucket
                ORDER BY b.local_time DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
)
SELECT
    asset_uuid::text AS asset_id,
    asset_name,
    local_time AS recorded_at,
    shift_1_count,
    shift_2_count,
    shift_3_count,
    total_count,
    payload_time,
    payload_time < (NOW() + INTERVAL '{offset} hours') - INTERVAL '{asset.nominal_range.get("max_stale_hours", 6)} hours' AS is_stale
FROM sampled_data
ORDER BY recorded_at ASC;
""".strip()

        return f"""
WITH raw_data AS (
    SELECT
        a.id AS asset_uuid,
        a.name AS asset_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        (ad.value->>'prod_shift_1_today')::numeric AS shift_1_count,
        (ad.value->>'prod_shift_2_today')::numeric AS shift_2_count,
        (ad.value->>'prod_shift_3_today')::numeric AS shift_3_count,
        (ad.value->>'prod_total_today')::numeric AS total_count,
        COALESCE(NULLIF(ad.value->>'Timestamp', '')::timestamp, ad.timestamp + INTERVAL '{offset} hours') AS payload_time
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN {self._settings.openremote_schema}.asset a ON a.id = ad.entity_id
    WHERE {exact_match}
      AND (ad.value->>'prod_total_today') IS NOT NULL
      {local_time_window_filter}
)
SELECT
    asset_uuid::text AS asset_id,
    asset_name,
    local_time AS recorded_at,
    shift_1_count,
    shift_2_count,
    shift_3_count,
    total_count,
    payload_time,
    payload_time < (NOW() + INTERVAL '{offset} hours') - INTERVAL '{asset.nominal_range.get("max_stale_hours", 6)} hours' AS is_stale
FROM raw_data
ORDER BY recorded_at DESC
LIMIT 500;
""".strip()

    def _build_tank_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        offset = self._settings.local_time_offset_hours
        interval = self._interval_sql(time_window)
        exact_match = self._exact_asset_match_sql(asset.name)
        local_time_window_filter = self._window_filter_sql(
            f"(ad.timestamp + INTERVAL '{offset} hours')",
            time_window,
            local=True,
        )
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH tank_data AS (
    SELECT
        a.id AS asset_uuid,
        a.name AS asset_name,
        ad.attribute_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        ROUND(
            regexp_replace(
                COALESCE(ad.value->>'TankOilLevelInFeet001', ad.value->>'Level'),
                '[^0-9\\.-]',
                '',
                'g'
            )::numeric,
            2
        ) AS tank_oil_level_in_feet_001
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN {self._settings.openremote_schema}.asset a ON a.id = ad.entity_id
    WHERE {exact_match}
      AND COALESCE(ad.value->>'TankOilLevelInFeet001', ad.value->>'Level') IS NOT NULL
      AND NULLIF(
            regexp_replace(
                COALESCE(ad.value->>'TankOilLevelInFeet001', ad.value->>'Level'),
                '[^0-9\\.-]',
                '',
                'g'
            ),
            ''
          ) IS NOT NULL
      {local_time_window_filter}
),
bucketed AS (
    SELECT
        td.*,
        date_trunc('day', td.local_time) AS day_bucket,
        ntile({per_day_points}) OVER (
            PARTITION BY td.asset_uuid, date_trunc('day', td.local_time)
            ORDER BY td.local_time
        ) AS sample_bucket
    FROM tank_data td
),
sampled_tank_data AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_uuid, b.day_bucket, b.sample_bucket
                ORDER BY b.local_time DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
),
latest_capacity AS (
    SELECT
        a.id AS asset_uuid,
        'asset.maxcapacity.value' AS attribute_name,
        NULLIF(a.attributes::jsonb->'maxcapacity'->'value'->>'MaxFt', '')::numeric AS max_ft
    FROM {self._settings.openremote_schema}.asset a
    WHERE {exact_match}
      AND NULLIF(a.attributes::jsonb->'maxcapacity'->'value'->>'MaxFt', '') IS NOT NULL
),
ordered AS (
    SELECT
           td.asset_uuid,
           td.asset_name,
           td.attribute_name AS level_attribute,
           td.raw_time,
           td.local_time,
           lc.attribute_name AS max_ft_attribute,
           lc.max_ft,
           td.tank_oil_level_in_feet_001,
           GREATEST(COALESCE(lc.max_ft, 0) - COALESCE(td.tank_oil_level_in_feet_001, 0), 0) AS current_level_ft,
           LAG(GREATEST(COALESCE(lc.max_ft, 0) - COALESCE(td.tank_oil_level_in_feet_001, 0), 0))
               OVER (PARTITION BY asset_name ORDER BY local_time ASC) AS previous_level
    FROM sampled_tank_data td
    LEFT JOIN latest_capacity lc ON lc.asset_uuid = td.asset_uuid
)
SELECT
    asset_uuid::text AS asset_id,
    asset_name,
    level_attribute,
    max_ft_attribute,
    local_time AS recorded_at,
    max_ft,
    tank_oil_level_in_feet_001,
    current_level_ft,
    ROUND((current_level_ft / NULLIF(max_ft, 0)) * 100, 1) AS percentage_filled,
    CASE
        WHEN previous_level IS NULL THEN 0
        ELSE ROUND(current_level_ft - previous_level, 2)
    END AS level_change,
    raw_time < NOW() - INTERVAL '{asset.nominal_range.get("max_stale_hours", 1)} hours' AS is_stale
FROM ordered
ORDER BY recorded_at ASC;
""".strip()

        return f"""
WITH tank_data AS (
    SELECT
        a.id AS asset_uuid,
        a.name AS asset_name,
        ad.attribute_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '{offset} hours') AS local_time,
        ROUND(
            regexp_replace(
                COALESCE(ad.value->>'TankOilLevelInFeet001', ad.value->>'Level'),
                '[^0-9\\.-]',
                '',
                'g'
            )::numeric,
            2
        ) AS tank_oil_level_in_feet_001
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    JOIN {self._settings.openremote_schema}.asset a ON a.id = ad.entity_id
    WHERE {exact_match}
      AND COALESCE(ad.value->>'TankOilLevelInFeet001', ad.value->>'Level') IS NOT NULL
      AND NULLIF(
            regexp_replace(
                COALESCE(ad.value->>'TankOilLevelInFeet001', ad.value->>'Level'),
                '[^0-9\\.-]',
                '',
                'g'
            ),
            ''
          ) IS NOT NULL
      {local_time_window_filter}
),
latest_capacity AS (
    SELECT
        a.id AS asset_uuid,
        'asset.maxcapacity.value' AS attribute_name,
        NULLIF(a.attributes::jsonb->'maxcapacity'->'value'->>'MaxFt', '')::numeric AS max_ft
    FROM {self._settings.openremote_schema}.asset a
    WHERE {exact_match}
      AND NULLIF(a.attributes::jsonb->'maxcapacity'->'value'->>'MaxFt', '') IS NOT NULL
),
ordered AS (
    SELECT
           td.asset_uuid,
           td.asset_name,
           td.attribute_name AS level_attribute,
           td.raw_time,
           td.local_time,
           lc.attribute_name AS max_ft_attribute,
           lc.max_ft,
           td.tank_oil_level_in_feet_001,
           GREATEST(COALESCE(lc.max_ft, 0) - COALESCE(td.tank_oil_level_in_feet_001, 0), 0) AS current_level_ft,
           LAG(GREATEST(COALESCE(lc.max_ft, 0) - COALESCE(td.tank_oil_level_in_feet_001, 0), 0))
               OVER (PARTITION BY asset_name ORDER BY local_time ASC) AS previous_level
    FROM tank_data td
    LEFT JOIN latest_capacity lc ON lc.asset_uuid = td.asset_uuid
)
SELECT
    asset_uuid::text AS asset_id,
    asset_name,
    level_attribute,
    max_ft_attribute,
    local_time AS recorded_at,
    max_ft,
    tank_oil_level_in_feet_001,
    current_level_ft,
    ROUND((current_level_ft / NULLIF(max_ft, 0)) * 100, 1) AS percentage_filled,
    CASE
        WHEN previous_level IS NULL THEN 0
        ELSE ROUND(current_level_ft - previous_level, 2)
    END AS level_change,
    raw_time < NOW() - INTERVAL '{asset.nominal_range.get("max_stale_hours", 1)} hours' AS is_stale
FROM ordered
ORDER BY recorded_at DESC
LIMIT 500;
""".strip()

    def _build_generic_query(self, asset: AssetProfile, time_window: TimeWindow) -> str:
        interval = self._interval_sql(time_window)
        exact_match = self._exact_asset_match_sql(asset.name)
        offset = self._settings.local_time_offset_hours
        timestamp_window_filter = self._window_filter_sql("ad.timestamp", time_window, local=False)
        sampled = self._is_multi_day_window(time_window)
        per_day_points = self._points_per_day(time_window)
        if sampled:
            return f"""
WITH raw_data AS (
    SELECT
        a.id::text AS asset_id,
        a.name AS asset_name,
        ad.attribute_name,
        (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
        ad.value AS sensor_data,
        date_trunc('day', ad.timestamp + INTERVAL '{offset} hours') AS day_bucket
    FROM {self._settings.openremote_schema}.asset a
    JOIN {self._settings.openremote_schema}.asset_datapoint ad ON ad.entity_id = a.id
    WHERE {exact_match}
      {timestamp_window_filter}
),
bucketed AS (
    SELECT
        rd.*,
        ntile({per_day_points}) OVER (
            PARTITION BY rd.asset_id, rd.attribute_name, rd.day_bucket
            ORDER BY rd.timestamp
        ) AS sample_bucket
    FROM raw_data rd
),
sampled_data AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_id, b.attribute_name, b.day_bucket, b.sample_bucket
                ORDER BY b.timestamp DESC
            ) AS rn
        FROM bucketed b
    ) ranked
    WHERE rn = 1
)
SELECT
    asset_id,
    asset_name,
    attribute_name,
    (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
    sensor_data
FROM sampled_data
ORDER BY timestamp ASC, attribute_name;
""".strip()
        return f"""
SELECT
    a.id::text AS asset_id,
    a.name AS asset_name,
    ad.attribute_name,
    (ad.timestamp + INTERVAL '{offset} hours') AS timestamp,
    ad.value AS sensor_data
FROM {self._settings.openremote_schema}.asset a
JOIN {self._settings.openremote_schema}.asset_datapoint ad ON ad.entity_id = a.id
WHERE {exact_match}
  {timestamp_window_filter}
ORDER BY ad.timestamp DESC
LIMIT 500;
""".strip()

    def _coalesce_json_value(self, keys: list[str]) -> str:
        expressions = [f"ad.value->>'{key}'" for key in keys]
        return f"COALESCE({', '.join(expressions)})"

    def _exact_asset_match_sql(self, name: str) -> str:
        escaped = self._escape_sql(name)
        return (
            "REPLACE(LOWER(a.name), ' ', '') = "
            f"REPLACE(LOWER('{escaped}'), ' ', '')"
        )

    def _interval_sql(self, time_window: TimeWindow) -> str:
        unit = time_window.unit.rstrip("s")
        value = max(1, time_window.value)
        return f"{value} {unit}"

    def _window_filter_sql(self, column_expr: str, time_window: TimeWindow, local: bool) -> str:
        bounds = self._absolute_window_bounds(time_window)
        offset = self._settings.local_time_offset_hours
        if bounds is not None:
            start_dt, end_dt = bounds
            start_text = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            end_text = end_dt.strftime("%Y-%m-%d %H:%M:%S")
            if not local:
                return (
                    f"AND {column_expr} >= TIMESTAMP '{start_text}' - INTERVAL '{offset} hours'\n"
                    f"      AND {column_expr} < TIMESTAMP '{end_text}' - INTERVAL '{offset} hours'"
                )
            return (
                f"AND {column_expr} >= TIMESTAMP '{start_text}'\n"
                f"      AND {column_expr} < TIMESTAMP '{end_text}'"
            )

        interval = self._interval_sql(time_window)
        if local:
            return (
                f"AND {column_expr} >= NOW() + INTERVAL '{offset} hours' - INTERVAL '{interval}'"
            )
        return f"AND {column_expr} >= NOW() - INTERVAL '{interval}'"

    def _absolute_window_bounds(self, time_window: TimeWindow) -> tuple[datetime, datetime] | None:
        if not time_window.start_at or not time_window.end_at:
            return None
        try:
            start_dt = datetime.fromisoformat(time_window.start_at)
            end_dt = datetime.fromisoformat(time_window.end_at)
            return start_dt, end_dt
        except ValueError:
            return None

    def _is_multi_day_window(self, time_window: TimeWindow) -> bool:
        if time_window.unit in {"days", "weeks", "months"}:
            return True
        return time_window.unit == "hours" and time_window.value >= 24

    def _points_per_day(self, time_window: TimeWindow) -> int:
        # Keep payload manageable while preserving trend shape for long windows.
        if time_window.unit == "months":
            return 10
        if time_window.unit in {"weeks", "days"}:
            return 12
        return 15

    def _escape_sql(self, value: str) -> str:
        return value.replace("'", "''")


nl_to_sql_service = NLToSQLService()
