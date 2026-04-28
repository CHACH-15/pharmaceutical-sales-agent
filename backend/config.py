from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── LLM ─────────────────────────────────────────
    groq_api_key: str = ""
    llm_base_url: str = "https://api.groq.com"
    llm_model: str = "llama-3.3-70b-versatile"          # main SQL agent model
    llm_enhancer_model: str = "llama-3.1-8b-instant"    # lightweight enhancer model (swap later)
    llm_temperature: float = 0.2
    llm_max_tokens: int = 2084
    llm_max_history_turns: int = 8
    llm_sql_max_retries: int = 2

    @property
    def llm_api_key(self) -> str:
        return self.groq_api_key

    # ── Server ───────────────────────────────────────
    host: str = "0.0.0.0"
    port: int = 8000

    allow_origins: List[str] = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
    
    # ── Admin ────────────────────────────────────────
    admin_email: str = "CHAHD123@hikma.com"
    admin_password: str = ""

    # ── Auth ─────────────────────────────────────────
    secret_key: str = "change-me-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 10080

    # ── DB ───────────────────────────────────────────
    db_path: str = "wisdom.db"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()