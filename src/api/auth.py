import os
import jwt
import requests
from fastapi import Depends, HTTPException, Header
from typing import Optional

# Ensure dotenv is loaded (should be loaded by main.py or uvicorn)
from dotenv import load_dotenv
load_dotenv()

CLERK_SECRET_KEY = os.environ.get("CLERK_SECRET_KEY")

async def get_current_user(authorization: Optional[str] = Header(None)):
    """
    Verify the user using Clerk API.
    Returns the user object if valid, else None.
    """
    if not authorization:
        return None
        
    if not authorization.startswith("Bearer "):
        return None
        
    token = authorization.split(" ")[1]
    
    if not CLERK_SECRET_KEY:
        print("WARNING: CLERK_SECRET_KEY not found in environment variables. Auth disabled.")
        return None

    try:
        # 1. Decode token (without verification) to get user_id (sub)
        # We rely on the Clerk API call to verify the session/user is actually active
        payload = jwt.decode(token, options={"verify_signature": False})
        user_id = payload.get("sub")
        
        if not user_id:
            return None
            
        # 2. Call Clerk API to verify user and get metadata
        # We use a cache-friendly user retrieval or session verify?
        # /v1/users/{user_id} gives us metadata directly.
        
        response = requests.get(
            f"https://api.clerk.com/v1/users/{user_id}",
            headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"}
        )
        
        if response.status_code == 200:
            user_data = response.json()
            return user_data
        else:
            print(f"Auth Error: Clerk API returned {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"Auth Exception: {e}")
        return None

def is_user_premium(user: Optional[dict]) -> bool:
    """Check if the user has premium status in public_metadata."""
    if not user:
        return False
        
    public_metadata = user.get("public_metadata", {})
    # Check for is_premium flag (handle both snake_case and camelCase)
    return (public_metadata.get("is_premium") is True) or (public_metadata.get("isPremium") is True)
