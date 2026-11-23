"""AI SRE intake skeleton: alert in, verified recovery out (M0: intake only)."""

from ai_sre.db import get_pool, init_schema
from ai_sre.main import create_app

__all__ = ["create_app", "get_pool", "init_schema"]
