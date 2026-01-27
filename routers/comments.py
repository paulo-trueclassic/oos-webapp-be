from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List
from datetime import datetime

from core.data_models import CommentCreate, CommentRead
from core.security import get_current_user, User
from core.bigquery_service import bigquery_service

router = APIRouter()

@router.post("/comments", response_model=CommentRead, status_code=201)
async def create_comment(
    comment: CommentCreate,
    current_user: User = Depends(get_current_user)
):
    """
    Create a new comment for an OOS SKU.
    """
    try:
        # The author is the currently authenticated user
        author = current_user.username # Assuming the user model has a 'username' field
        
        # Create the full comment record to be inserted
        comment_data_with_author = CommentRead(
            **comment.dict(),
            author=author,
            created_at=datetime.utcnow()
        )
        
        # Add the comment to BigQuery
        bigquery_service.add_comment_to_bigquery(comment_data_with_author)
        
        return comment_data_with_author
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create comment: {str(e)}")

@router.get("/comments", response_model=List[CommentRead])
async def get_comments(
    order_id: str = Query(..., description="The ID of the order to fetch comments for."),
    sku: str = Query(..., description="The SKU to fetch comments for."),
    current_user: User = Depends(get_current_user)
):
    """
    Retrieve comments for a specific order and SKU.
    """
    try:
        comments = bigquery_service.get_comments_from_bigquery(order_id=order_id, sku=sku)
        return comments
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve comments: {str(e)}")
