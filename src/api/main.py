from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routes import bets

app = FastAPI(
    title="Sports Betting API",
    description="API for serving sports betting predictions and history",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Next.js default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(bets.router)

@app.get("/")
async def root():
    return {"message": "Sports Betting API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
