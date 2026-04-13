"""
Quick test to verify Supabase connection.
Run: python test_supabase.py
"""
from supabase_client import supabase

try:
    response = supabase.table("trades").select("*").execute()
    print("Connection successful!")
    print(f"Response: {response}")
except Exception as e:
    print(f"Connection error: {e}")
    print("This is expected if the 'trades' table doesn't exist yet.")
    print("The important thing is that the client initialized without errors.")
