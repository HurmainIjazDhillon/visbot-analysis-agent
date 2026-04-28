from fastapi import APIRouter

from app.models.schemas import ChatRequest, ChatResponse
from app.services.analysis_agent import analysis_agent_service


router = APIRouter(tags=["chat"])


@router.post("/chat/message", response_model=ChatResponse)
async def chat_message(payload: ChatRequest) -> ChatResponse:
    return await analysis_agent_service.run_chat(payload)
