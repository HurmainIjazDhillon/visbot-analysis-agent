from __future__ import annotations

import time
from collections import Counter

from app.core.config import get_settings
from app.models.schemas import AssetProfile
from app.services.asset_catalog import asset_catalog_service
from app.services.data_repository import DataQueryError, data_repository


class LiveAssetRegistry:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._cache: list[AssetProfile] = []
        self._loaded_at = 0.0
        self._ttl_seconds = 60.0

    def list_assets(self, force_refresh: bool = False) -> list[AssetProfile]:
        now = time.time()
        if force_refresh or not self._cache or now - self._loaded_at > self._ttl_seconds:
            self._cache = self._load_assets()
            self._loaded_at = now
        return self._cache

    def get_asset(self, asset_id: str) -> AssetProfile | None:
        for asset in self.list_assets():
            aliases = [asset.asset_id, asset.db_asset_id, asset.name, *asset.lookup_names]
            if asset_id in aliases:
                return asset
        return None

    def find_candidates(self, message: str, limit: int = 12) -> list[AssetProfile]:
        normalized_message = self._normalize(message)
        message_tokens = self._tokenize(message)
        scored: list[tuple[int, int, int, int, int, str, AssetProfile]] = []

        for asset in self.list_assets():
            score = 0
            aliases = [asset.name, *asset.lookup_names]
            for alias in aliases:
                normalized_alias = self._normalize(alias)
                if not normalized_alias:
                    continue
                if normalized_alias == normalized_message:
                    score = max(score, 1000)
                elif normalized_alias in normalized_message:
                    score = max(score, 850 + len(normalized_alias))
                else:
                    alias_tokens = self._tokenize(alias)
                    overlap = len(set(alias_tokens) & set(message_tokens))
                    if overlap:
                        score = max(score, 100 + overlap * 30)

            if asset.parent_name:
                parent_overlap = len(set(self._tokenize(asset.parent_name)) & set(message_tokens))
                score += parent_overlap * 10

            if score:
                scored.append((
                    score,
                    1 if asset.has_live_data else 0,
                    asset.child_count,
                    1 if asset.is_device else 0,
                    len(asset.name),
                    asset.db_asset_id or asset.asset_id,
                    asset,
                ))

        scored.sort(key=lambda item: item[:-1], reverse=True)
        return [asset for *_, asset in scored[:limit]]

    def best_direct_match(self, message: str) -> AssetProfile | None:
        normalized_message = self._normalize(message)
        best: tuple[int, int, int, int, int, str, AssetProfile] | None = None

        for asset in self.list_assets():
            aliases = [asset.name, *asset.lookup_names]
            best_alias_score = 0
            for alias in aliases:
                normalized_alias = self._normalize(alias)
                if not normalized_alias:
                    continue
                if normalized_alias == normalized_message:
                    best_alias_score = max(best_alias_score, 5000 + len(normalized_alias) * 5)
                elif normalized_alias in normalized_message:
                    best_alias_score = max(best_alias_score, 3000 + len(normalized_alias) * 5)

            if best_alias_score:
                candidate = (
                    best_alias_score,
                    1 if asset.has_live_data else 0,
                    asset.child_count,
                    1 if asset.is_device else 0,
                    len(asset.name),
                    asset.db_asset_id or asset.asset_id,
                    asset,
                )
                if best is None or candidate[:-1] > best[:-1]:
                    best = candidate

        return best[-1] if best else None

    def _load_assets(self) -> list[AssetProfile]:
        query = f"""
WITH latest_data AS (
    SELECT DISTINCT ON (ad.entity_id, ad.attribute_name)
        ad.entity_id,
        ad.attribute_name,
        ad.value,
        ad.timestamp
    FROM {self._settings.openremote_schema}.asset_datapoint ad
    WHERE jsonb_typeof(ad.value::jsonb) = 'object'
    ORDER BY ad.entity_id, ad.attribute_name, ad.timestamp DESC
),
latest_entities AS (
    SELECT DISTINCT entity_id
    FROM latest_data
),
child_counts AS (
    SELECT parent_id, COUNT(*) AS child_count
    FROM {self._settings.openremote_schema}.asset
    WHERE parent_id IS NOT NULL
    GROUP BY parent_id
),
asset_attributes AS (
    SELECT
        a.id,
        CASE
            WHEN a.attributes IS NULL OR jsonb_typeof(a.attributes::jsonb) != 'object' THEN ARRAY[]::text[]
            ELSE ARRAY(
                SELECT key
                FROM jsonb_object_keys(a.attributes::jsonb) AS key
                ORDER BY key
            )
        END AS attribute_keys,
        CASE
            WHEN a.attributes IS NULL OR jsonb_typeof(a.attributes::jsonb) != 'object' THEN FALSE
            ELSE ((a.attributes::jsonb - 'notes' - 'location') != '{{}}'::jsonb)
        END AS is_device
    FROM {self._settings.openremote_schema}.asset a
)
SELECT
    a.id::text AS db_asset_id,
    a.name,
    COALESCE(parent.name, '') AS parent_name,
    COALESCE(cc.child_count, 0) AS child_count,
    COALESCE(aa.is_device, FALSE) AS is_device,
    COALESCE(aa.attribute_keys, ARRAY[]::text[]) AS attribute_keys,
    (ld.entity_id IS NOT NULL) AS has_live_data,
    COALESCE(
        (
            SELECT array_agg(DISTINCT key ORDER BY key)
            FROM latest_data ld2,
            LATERAL jsonb_object_keys(
                CASE
                    WHEN jsonb_typeof(ld2.value::jsonb) = 'object' THEN ld2.value::jsonb
                    ELSE '{{}}'::jsonb
                END
            ) AS key
            WHERE ld2.entity_id = a.id
        ),
        ARRAY[]::text[]
    ) AS data_keys
FROM {self._settings.openremote_schema}.asset a
LEFT JOIN {self._settings.openremote_schema}.asset parent ON parent.id = a.parent_id
LEFT JOIN child_counts cc ON cc.parent_id = a.id
LEFT JOIN asset_attributes aa ON aa.id = a.id
LEFT JOIN latest_entities ld ON ld.entity_id = a.id
ORDER BY a.name;
""".strip()
        rows = data_repository.execute_query(query)
        return [self._merge_with_override(row) for row in rows]

    def _merge_with_override(self, row: dict) -> AssetProfile:
        name = str(row.get("name", ""))
        override = asset_catalog_service.get_asset_by_name(name)
        inferred_type = self._infer_asset_type(name, str(row.get("parent_name", "")), row.get("data_keys") or [])
        is_device = bool(row.get("is_device"))
        default_supported = self._default_supported_analyses(
            inferred_type,
            bool(row.get("has_live_data")),
            is_device,
            int(row.get("child_count", 0) or 0),
        )
        data_keys = [str(item) for item in (row.get("data_keys") or [])]
        attribute_keys = [str(item) for item in (row.get("attribute_keys") or [])]

        base = AssetProfile(
            asset_id=str(row.get("db_asset_id", name)),
            db_asset_id=str(row.get("db_asset_id", "")),
            name=name,
            parent_name=str(row.get("parent_name", "")),
            child_count=int(row.get("child_count", 0) or 0),
            is_device=is_device,
            attribute_keys=attribute_keys,
            has_live_data=bool(row.get("has_live_data")),
            data_keys=data_keys,
            asset_type=inferred_type,
            description=self._build_description(
                name,
                str(row.get("parent_name", "")),
                data_keys,
                bool(row.get("has_live_data")),
                is_device,
                attribute_keys,
            ),
            supported_analyses=default_supported,
            metrics=data_keys[:12],
            source_table=f"{self._settings.openremote_schema}.asset_datapoint",
            analysis_instructions=self._build_analysis_instructions(
                name,
                str(row.get("parent_name", "")),
                inferred_type,
                data_keys,
                bool(row.get("has_live_data")),
                is_device,
                int(row.get("child_count", 0) or 0),
            ),
            lookup_names=[name],
        )

        if override is None:
            return base

        merged = base.model_copy(update={
            "asset_id": override.asset_id or base.asset_id,
            "asset_type": override.asset_type or base.asset_type,
            "description": override.description or base.description,
            "supported_analyses": override.supported_analyses or base.supported_analyses,
            "metrics": override.metrics or base.metrics,
            "analysis_instructions": override.analysis_instructions or base.analysis_instructions,
            "sql_notes": override.sql_notes or base.sql_notes,
            "lookup_names": sorted(set([base.name, *base.lookup_names, *override.lookup_names])),
            "temperature_keys": override.temperature_keys,
            "humidity_keys": override.humidity_keys,
            "level_keys": override.level_keys,
            "production_keys": override.production_keys,
            "nominal_range": override.nominal_range or base.nominal_range,
        })
        return merged

    def _infer_asset_type(self, name: str, parent_name: str, data_keys: list[str]) -> str:
        lowered = f"{name} {parent_name}".lower()
        key_counts = Counter(key.lower() for key in data_keys)
        smoke_keys = {"warn", "temp", "humi", "bat_percent", "bat_voltage"}
        if "tank level monitoring" in lowered:
            return "tank"
        if "weather station" in lowered:
            return "weather_station"
        if "smoke alarm" in lowered or smoke_keys & set(key_counts):
            return "smoke_alarm"
        if "coldroom" in lowered or {"temperature", "humidity"} & set(key_counts):
            return "cold_room"
        if "machine" in lowered or any("prod_" in key for key in key_counts):
            return "filling_machine"
        if "tank" in lowered or {"tankoillevelinfeet001", "level"} & set(key_counts):
            return "tank"
        if "boiler" in lowered:
            return "boiler"
        if "energy meter" in lowered or "power meter" in lowered or {"voltage", "current", "power", "energy"} & set(key_counts):
            return "energy_meter"
        return "generic_asset"

    def _default_supported_analyses(self, asset_type: str, has_live_data: bool, is_device: bool, child_count: int) -> list[str]:
        if not is_device and child_count > 0:
            return ["hierarchy_summary", "group_status", "group_comparison"]
        if asset_type == "cold_room":
            return ["live_status", "consumption_analysis", "cooling_cycle_analysis", "historical_variance", "anomaly_detection"]
        if asset_type == "filling_machine":
            return ["live_status", "throughput_utilization", "historical_variance", "anomaly_detection"]
        if asset_type == "tank":
            return ["live_status", "inventory_level_analysis", "trend_analysis", "anomaly_detection"]
        if asset_type == "weather_station":
            return ["live_status", "trend_analysis", "anomaly_detection"]
        if asset_type == "energy_meter":
            return ["live_status", "trend_analysis", "group_status", "historical_variance", "consumption_analysis"]
        if asset_type == "smoke_alarm":
            return ["live_status", "anomaly_detection", "group_status"]
        if asset_type == "boiler":
            return ["live_status", "trend_analysis", "historical_variance", "anomaly_detection"]
        return ["live_status", "trend_analysis"] if has_live_data else ["hierarchy_summary"]

    def _build_description(
        self,
        name: str,
        parent_name: str,
        data_keys: list[str],
        has_live_data: bool,
        is_device: bool,
        attribute_keys: list[str],
    ) -> str:
        role_text = "sensor device" if is_device else "category or grouping asset"
        live_text = "has live telemetry data" if has_live_data else "does not appear to have direct live telemetry"
        parent_text = f"under parent {parent_name}" if parent_name else "with no recorded parent in the current view"
        attr_text = f"Attribute keys include: {', '.join(attribute_keys[:12])}." if attribute_keys else "No attribute keys were discovered."
        keys_text = f"Available telemetry keys include: {', '.join(data_keys[:12])}." if data_keys else "No telemetry keys were discovered yet."
        return f"{name} is an OpenRemote {role_text} {parent_text}. It {live_text}. {attr_text} {keys_text}"

    def _build_analysis_instructions(
        self,
        name: str,
        parent_name: str,
        asset_type: str,
        data_keys: list[str],
        has_live_data: bool,
        is_device: bool,
        child_count: int,
    ) -> str:
        if not is_device and child_count > 0:
            return (
                f"{name} is a category/group asset. It should be analyzed through its child assets. "
                "If the user asks about this asset, first discover the relevant child devices under the hierarchy "
                "and summarize those instead of expecting direct telemetry on the category itself."
            )
        if asset_type == "tank":
            return (
                f"Analyze {name} as a tank-type device. "
                "For tank devices, actual physical liquid height is MaxFt - TankOilLevelInFeet001, "
                "even when those values come from different attributes such as Data and Maxcapacity. "
                "Use that derived height in the final analysis whenever those fields are available."
            )
        if asset_type == "cold_room" and "t & h monitoring" in f"{name} {parent_name}".lower():
            return (
                f"Analyze {name} as a T & H Monitoring cold-room device. "
                "Treat the safe temperature threshold as 5C to 45C unless the user explicitly provides a stricter range. "
                "Always provide consumption analysis for the requested time window by inferring ON/OFF state from temperature history. "
                "A cooling cycle starts at the last temperature peak before a drop that later goes below 20C, "
                "stays ON while temperature remains below 20C, and ends at the valley before temperature rises back to 20C or above. "
                "Sum those cycle durations and clip any carry-over cycle to the 6:00 AM shift start."
            )
        if asset_type == "smoke_alarm":
            return (
                f"Analyze {name} as a smoke-alarm sensor or smoke-alarm group. "
                "Inspect all JSON telemetry attributes for warn, temp, humi, bat_percent, and bat_voltage values. "
                "Treat warn='warn' as smoke/fire detected, warn='mute' as alarm muted after fire detection, "
                "warn='fault' as sensor fault, warn='remove' as sensor removal, warn='install' as sensor install event, "
                "warn='low-vol' or warn='low-vol-test' as low-battery/voltage conditions, and warn='ok' as normal. "
                "If the user asks about a category, summarize all child devices together and list affected devices with timestamps."
            )
        return (
            f"Analyze {name} using the returned telemetry keys {', '.join(data_keys[:12])}. "
            "Use the user's requested time range and produce a practical end-user summary."
        )

    def _normalize(self, value: str) -> str:
        return "".join(value.lower().split())

    def _tokenize(self, value: str) -> list[str]:
        return [token for token in self._normalize_for_tokens(value).split() if token]

    def _normalize_for_tokens(self, value: str) -> str:
        cleaned = "".join(char if char.isalnum() else " " for char in value.lower())
        return " ".join(cleaned.split())


live_asset_registry = LiveAssetRegistry()
