from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.analysis import router as analysis_router
from app.api.routes.assets import router as assets_router
from app.api.routes.chat import router as chat_router


app = FastAPI(
    title="VisBot Analysis",
    version="0.1.0",
    description="Intelligent asset analysis agent powered by FastAPI, LangChain, and Groq.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(assets_router, prefix="/api/v1")
app.include_router(chat_router, prefix="/api/v1")
app.include_router(analysis_router, prefix="/api/v1")
