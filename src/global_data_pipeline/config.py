from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Paths
    data_dir: Path = Path("./data")

    # Hugging Face
    hf_token: str | None = None

    # UN COMTRADE (Phase 2)
    comtrade_subscription_key: str | None = None

    # Neon PostgreSQL (Phase 3)
    database_url: str | None = None

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def datasets_dir(self) -> Path:
        return self.data_dir / "datasets"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"

    def ensure_dirs(self) -> None:
        for d in (self.raw_dir, self.datasets_dir, self.state_dir):
            d.mkdir(parents=True, exist_ok=True)


settings = Settings()
