import uuid
from passlib.hash import bcrypt
from supabase_client import supabase


def register_user(username, password):
    """
    Registers a new user in the Supabase 'users' table.
    Hashes password and generates a unique user_id.
    """
    # Check if username exists
    try:
        existing = supabase.table("users") \
            .select("*") \
            .eq("username", username) \
            .execute()

        if existing.data:
            return {"error": "Username already exists"}

        user_id = str(uuid.uuid4())
        hashed_password = bcrypt.hash(password)

        data = {
            "user_id": user_id,
            "username": username,
            "password": hashed_password
        }

        supabase.table("users").insert(data).execute()
        return {"status": "registered", "user_id": user_id}
        
    except Exception as e:
        import logging
        logging.error(f"Registration error: {e}")
        return {"error": str(e)}


def login_user(username, password):
    """
    Verifies user credentials against the Supabase 'users' table.
    """
    try:
        res = supabase.table("users") \
            .select("*") \
            .eq("username", username) \
            .execute()

        if not res.data:
            return None

        user = res.data[0]

        if bcrypt.verify(password, user["password"]):
            return user
        else:
            return None
            
    except Exception as e:
        import logging
        logging.error(f"Login error: {e}")
        return None
