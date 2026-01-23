from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from core.auth_config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
from core.user_service import user_service
from core.logger import get_logger

logger = get_logger(__name__)

# --- Password Hashing ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a hashed one."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Hashes a plain password."""
    return pwd_context.hash(password)

# --- JWT Token Handling ---
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class TokenData(BaseModel):
    username: Optional[str] = None

class User(BaseModel):
    username: str
    role: str

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Creates a new JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- FastAPI Dependencies for Authentication & Authorization ---

async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """
    Decodes the JWT token to get the current user.
    Raises credentials_exception if the token is invalid.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            logger.warning("Token decoding failed: 'sub' claim missing.")
            raise credentials_exception
        
        # Fetch user from the database to ensure they still exist and get their role
        user_data = user_service.get_user_by_username(username)
        if user_data is None:
            logger.warning(f"User '{username}' from token not found in database.")
            raise credentials_exception
            
        return User(**user_data)
        
    except JWTError as e:
        logger.error(f"JWT Error during token decoding: {e}")
        raise credentials_exception
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_current_user: {e}")
        raise credentials_exception


async def is_admin(current_user: User = Depends(get_current_user)) -> User:
    """
    FastAPI dependency that checks if the current user has the 'admin' role.
    Raises an HTTPException if the user is not an admin.
    """
    if current_user.role != "admin":
        logger.warning(f"Authorization failed: User '{current_user.username}' with role '{current_user.role}' tried to access an admin-only endpoint.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The user does not have adequate permissions",
        )
    return current_user
