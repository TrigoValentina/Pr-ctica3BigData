from supabase import create_client
import os

SUPABASE_URL = "https://ugqhpqllxrcjyusslasg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVncWhwcWxseHJjanl1c3NsYXNnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjMyNDE0NDgsImV4cCI6MjA3ODgxNzQ0OH0.bwVIZf6bCqL1cuYZwFvwgysLZvDv2LzyvgxcLEpDA0U"  # usa la ANON KEY

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
