from supabase import create_client, Client
from dotenv import load_dotenv
import os

"""Utilidades de conexión a Supabase."""
from supabase import create_client, Client
from dotenv import load_dotenv
import os

def get_client() -> Client:

    load_dotenv()  
    url: str | None = os.getenv("SUPABASE_URL")
    key: str | None = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise RuntimeError(
            "Faltan SUPABASE_URL o SUPABASE_KEY en el entorno / .env"
        )

    return create_client(url, key)