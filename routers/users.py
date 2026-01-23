from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, constr
from typing import List

from core.user_service import user_service, BigQueryClientError
from core.security import get_password_hash, get_current_user, is_admin, User
from core.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/api/users", tags=["User Management"])

# --- Pydantic Models ---
class UserCreate(BaseModel):
    username: constr(strip_whitespace=True, min_length=3)
    password: str
    role: constr(strip_whitespace=True, pattern=r"^(admin|user)$") # Must be 'admin' or 'user'

class UserOut(BaseModel):
    username: str
    role: str

class PasswordReset(BaseModel):
    new_password: str

class SelfPasswordReset(PasswordReset):
    current_password: str

# --- Admin Endpoints ---

@router.post("/", response_model=UserOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(is_admin)])
async def create_new_user(user: UserCreate):
    """
    [Admin Only] Create a new user.
    """
    try:
        hashed_password = get_password_hash(user.password)
        created_user = user_service.create_user(
            username=user.username,
            hashed_password=hashed_password,
            role=user.role
        )
        return created_user
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except BigQueryClientError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/", response_model=List[UserOut], dependencies=[Depends(is_admin)])
async def get_all_users():
    """
    [Admin Only] Get a list of all users.
    """
    try:
        return user_service.get_all_users()
    except BigQueryClientError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.put("/{username}/force-reset-password", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(is_admin)])
async def admin_reset_user_password(username: str, password_data: PasswordReset):
    """
    [Admin Only] Force-reset a password for any user.
    """
    try:
        new_hashed_password = get_password_hash(password_data.new_password)
        success = user_service.update_password(username, new_hashed_password)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User '{username}' not found.")
    except BigQueryClientError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.delete("/{username}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(is_admin)])
async def delete_user_by_username(username: str, current_admin: User = Depends(is_admin)):
    """
    [Admin Only] Delete a user.
    """
    if username == current_admin.username:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Admins cannot delete their own account.")
    
    try:
        success = user_service.delete_user(username)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User '{username}' not found.")
    except BigQueryClientError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# --- Current User Endpoints ---

@router.put("/me/reset-password", status_code=status.HTTP_204_NO_CONTENT)
async def current_user_reset_password(password_data: SelfPasswordReset, current_user: User = Depends(get_current_user)):
    """
    [All Logged-in Users] Reset the password for the currently authenticated user.
    """
    from core.security import verify_password
    
    # Verify current password
    user_db = user_service.get_user_by_username(current_user.username)
    if not user_db or not verify_password(password_data.current_password, user_db.get("hashed_password")):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Incorrect current password.")
        
    try:
        new_hashed_password = get_password_hash(password_data.new_password)
        user_service.update_password(current_user.username, new_hashed_password)
    except BigQueryClientError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/me", response_model=UserOut)
async def read_users_me(current_user: User = Depends(get_current_user)):
    """
    [All Logged-in Users] Get details for the currently authenticated user.
    """
    return current_user
