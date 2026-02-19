"""
Super simple authentication (no JWT, no hashing)
Supports both username and email for login/register
"""
from database import get_all_users, user_exists_by_username, user_exists_by_email, create_user, get_user

def register(identifier: str, password: str) -> dict:
    """Register new user with username or email"""
    exists, _ = user_exists_by_username(identifier)
    if exists:
        return {"error": "Username/email already registered"}

    username = create_user(identifier, password)

    return {
        "success": True,
        "username": username,
        "email": identifier
    }

def login(identifier: str, password: str) -> dict:
    """Login user with username or email"""
    exists, username = user_exists_by_username(identifier)
    if not exists:
        return {"error": "Username/email not found"}

    user = get_user(username)
    if user['password'] != password:
        return {"error": "Wrong password"}

    return {
        "success": True,
        "username": username,
        "email": identifier
    }