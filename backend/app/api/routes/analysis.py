from fastapi import APIRouter

from app.models.schemas import AnalysisRequest, AnalysisResponse
from app.services.analysis_agent import analysis_agent_service


router = APIRouter(tags=["analysis"])


@router.post("/analysis/run", response_model=AnalysisResponse)
async def run_analysis(payload: AnalysisRequest) -> AnalysisResponse:
    return await analysis_agent_service.run_analysis(payload)
