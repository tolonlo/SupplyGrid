"""
Paquete chaos — Inyección de fallas configurables para SupplyGrid (Entregable 3).
"""

from .config import ChaosConfigManager
from .engine import (
    ChaosDatabaseError,
    ChaosEngine,
    ChaosException,
    ChaosHttpError,
    ChaosTimeoutError,
)
from .interceptor import ChaosConnection, ChaosCursor, connect_with_chaos, with_chaos
from .models import ChaosGlobalConfig, FaultConfig, FaultFamily, FaultType

__all__ = [
    "ChaosEngine",
    "ChaosConfigManager",
    "ChaosGlobalConfig",
    "FaultConfig",
    "FaultFamily",
    "FaultType",
    "ChaosException",
    "ChaosDatabaseError",
    "ChaosTimeoutError",
    "ChaosHttpError",
    "ChaosConnection",
    "ChaosCursor",
    "connect_with_chaos",
    "with_chaos",
]
