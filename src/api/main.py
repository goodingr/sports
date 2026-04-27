from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api import readiness
from src.api.routes import bets
from src.api.settings import get_settings

app = FastAPI(
    title="Sports Betting API",
    description="API for serving sports betting predictions and history",
    version="1.0.0",
)

settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(bets.router)
app.include_router(readiness.router)

@app.get("/")
async def root():
    return {"message": "Sports Betting API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
