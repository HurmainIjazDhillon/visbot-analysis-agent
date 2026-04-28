from __future__ import annotations

from pathlib import Path

import yaml

from app.models.schemas import AssetProfile


class AssetCatalogService:
    def __init__(self) -> None:
        self._catalog_path = Path(__file__).resolve().parents[2] / "data" / "asset_catalog.yaml"
        self._cache: dict[str, AssetProfile] | None = None

    def _load(self) -> dict[str, AssetProfile]:
        raw = yaml.safe_load(self._catalog_path.read_text(encoding="utf-8"))
        assets = raw.get("assets", [])
        return {
            asset["asset_id"]: AssetProfile(**asset)
            for asset in assets
        }

    def list_assets(self) -> list[AssetProfile]:
        self._cache = self._load()
        return list(self._cache.values())

    def get_asset(self, asset_id: str) -> AssetProfile | None:
        self._cache = self._load()
        return self._cache.get(asset_id)

    def get_asset_by_name(self, name: str) -> AssetProfile | None:
        normalized_name = self._normalize(name)
        for asset in self.list_assets():
            aliases = [asset.name, asset.asset_id, *asset.lookup_names]
            if any(self._normalize(alias) == normalized_name for alias in aliases):
                return asset
        return None

    def find_asset_by_message(self, message: str) -> AssetProfile | None:
        normalized_message = self._normalize(message)
        best_match: AssetProfile | None = None
        best_length = -1

        for asset in self.list_assets():
            aliases = [asset.asset_id, asset.name, *asset.lookup_names]
            for alias in aliases:
                normalized_alias = self._normalize(alias)
                if normalized_alias and normalized_alias in normalized_message:
                    if len(normalized_alias) > best_length:
                        best_match = asset
                        best_length = len(normalized_alias)
        return best_match

    def list_asset_names(self) -> list[str]:
        names: list[str] = []
        for asset in self.list_assets():
            names.append(asset.name)
            names.extend(asset.lookup_names)
        return sorted(set(names))

    def _normalize(self, value: str) -> str:
        return "".join(value.lower().split())


asset_catalog_service = AssetCatalogService()
