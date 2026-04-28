from fastapi import APIRouter, HTTPException

from app.services.live_asset_registry import live_asset_registry


router = APIRouter(tags=["assets"])


@router.get("/assets")
async def list_assets() -> list[dict]:
    return [asset.model_dump() for asset in live_asset_registry.list_assets()]


@router.get("/assets/{asset_id}")
async def get_asset(asset_id: str) -> dict:
    asset = live_asset_registry.get_asset(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Unknown asset: {asset_id}")
    return asset.model_dump()
