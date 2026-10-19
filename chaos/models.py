"""
Modelos de datos y esquemas para fallas configurables de SupplyGrid.
Cumple con las 5 condiciones de falla configurable (Rúbrica EAFIT):
1. Sin redespliegue (hot reload vía archivo / memoria).
2. Parametrizada (intensidad, probabilidad, duración, umbrales).
3. Focalizable (target: Q1-Q5, endpoint, tabla, % de tráfico).
4. Reversible (kill switch inmediato en segundos).
5. Reproducible (semilla pseudoaleatoria determinista).
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class FaultFamily(str, Enum):
    LATENCY = "LATENCY"                # Familia A: Latencia inducida
    DATABASE = "DATABASE"              # Familia B: Falla de base de datos
    DEGRADATION = "DEGRADATION"        # Familia C: Degradación de tiempos / recursos
    AVAILABILITY = "AVAILABILITY"      # Familia D: Riesgo de disponibilidad


class FaultType(str, Enum):
    # Familia A
    LATENCY_JITTER = "latency_jitter"
    CONTROLLED_TIMEOUT = "controlled_timeout"

    # Familia B
    DB_POOL_EXHAUSTION = "db_pool_exhaustion"
    RESOURCE_LOCK = "resource_lock"

    # Familia C
    CPU_BURN = "cpu_burn"
    MEMORY_PRESSURE = "memory_pressure"

    # Familia D
    NETWORK_PARTITION = "network_partition"
    EXTERNAL_5XX = "external_5xx"


@dataclass
class FaultConfig:
    id: str
    family: FaultFamily
    fault_type: FaultType
    enabled: bool = False
    description: str = ""
    # Focalización
    targets: List[str] = field(default_factory=lambda: ["*"])  # Ej: ["Q1", "Q4"], ["POST /ordenes-compra"]
    traffic_pct: float = 1.0  # 0.0 a 1.0 (100% de tráfico que coincide con el target)
    seed: Optional[int] = 42

    # Parámetros variables
    parameters: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> List[str]:
        """Valida límites y tipos de parámetros."""
        errors = []
        if not (0.0 <= self.traffic_pct <= 1.0):
            errors.append(f"traffic_pct debe estar entre 0.0 y 1.0, recibido: {self.traffic_pct}")

        params = self.parameters
        if self.fault_type == FaultType.LATENCY_JITTER:
            mean = params.get("mean_ms", 50.0)
            jitter = params.get("jitter_ms", 10.0)
            if mean < 0 or jitter < 0:
                errors.append("mean_ms y jitter_ms deben ser no negativos")

        elif self.fault_type == FaultType.CONTROLLED_TIMEOUT:
            timeout = params.get("timeout_ms", 100.0)
            if timeout <= 0:
                errors.append("timeout_ms debe ser positivo")

        elif self.fault_type == FaultType.DB_POOL_EXHAUSTION:
            prob = params.get("rejection_probability", 0.5)
            if not (0.0 <= prob <= 1.0):
                errors.append("rejection_probability debe estar entre 0.0 y 1.0")

        elif self.fault_type == FaultType.RESOURCE_LOCK:
            delay = params.get("lock_wait_ms", 200.0)
            if delay < 0:
                errors.append("lock_wait_ms debe ser >= 0")

        elif self.fault_type == FaultType.CPU_BURN:
            intensity = params.get("intensity_pct", 50.0)
            duration = params.get("duration_s", 5.0)
            if not (0.0 <= intensity <= 100.0):
                errors.append("intensity_pct debe estar entre 0.0 y 100.0")
            if duration <= 0:
                errors.append("duration_s debe ser positivo")

        elif self.fault_type == FaultType.MEMORY_PRESSURE:
            mb = params.get("size_mb", 100.0)
            if mb < 0 or mb > 2048:
                errors.append("size_mb debe estar entre 0 y 2048 MB")

        elif self.fault_type == FaultType.NETWORK_PARTITION:
            duration = params.get("duration_s", 5.0)
            if duration <= 0:
                errors.append("duration_s debe ser positivo")

        elif self.fault_type == FaultType.EXTERNAL_5XX:
            code = params.get("status_code", 500)
            if code not in (500, 502, 503, 504):
                errors.append("status_code debe ser un código 5xx (500, 502, 503, 504)")

        return errors


@dataclass
class ChaosGlobalConfig:
    kill_switch: bool = False
    enabled: bool = False
    faults: Dict[str, FaultConfig] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
