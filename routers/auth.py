from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from core.user_service import user_service
from core.security import verify_password, create_access_token
from core.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()

@router.post("/token", tags=["Authentication"])
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Provides a JWT token for valid credentials.
    Standard OAuth2 password flow. Expects form data with 'username' and 'password'.
    """
    user = user_service.get_user_by_username(form_data.username)
    if not user or not verify_password(form_data.password, user.get("hashed_password")):
        logger.warning(f"Failed login attempt for username: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token(
        data={"sub": user.get("username"), "role": user.get("role")}
    )
    logger.info(f"Successfully generated access token for user: {user.get('username')}")
    return {"access_token": access_token, "token_type": "bearer"}
