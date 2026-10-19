"""
Gestor dinámico de configuración para inyección de fallas en SupplyGrid.
Soporta recarga en caliente (hot reload) por detección de mtime de archivo
y API programática en memoria, sin requerir redespliegue ni reinicio de servicios.
"""

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .models import ChaosGlobalConfig, FaultConfig, FaultFamily, FaultType

DEFAULT_CONFIG_PATH = Path("chaos_config.json")
# Segundos minimos entre revisiones del archivo (hot reload). 0 = revisar siempre.
INTERVALO_REVISION_S = float(os.environ.get("CHAOS_REVISION_S", "0.05"))


def _crear_catalogo_predeterminado() -> Dict[str, FaultConfig]:
    """Crea el catálogo inicial con las 4 familias cubiertas (1-2 fallas por familia),
    todas desactivadas por defecto según las reglas de seguridad."""
    return {
        # Familia A: Latencia inducida
        "A1_latencia_jitter": FaultConfig(
            id="A1_latencia_jitter",
            family=FaultFamily.LATENCY,
            fault_type=FaultType.LATENCY_JITTER,
            enabled=False,
            description="Latencia fija con jitter lognormal sobre consultas o endpoints seleccionados.",
            targets=["Q1", "Q4", "POST /ordenes-compra"],
            traffic_pct=1.0,
            seed=42,
            parameters={
                "mean_ms": 25.0,
                "jitter_ms": 5.0,
                "distribution": "lognormal",
            },
        ),
        "A2_timeout_controlado": FaultConfig(
            id="A2_timeout_controlado",
            family=FaultFamily.LATENCY,
            fault_type=FaultType.CONTROLLED_TIMEOUT,
            enabled=False,
            description="Interrupción controlada por timeout si la ejecución excede el umbral límite.",
            targets=["Q4", "POST /ordenes-compra"],
            traffic_pct=0.5,
            seed=42,
            parameters={
                "timeout_ms": 15.0,
            },
        ),

        # Familia B: Falla de base de datos
        "B1_pool_agotado": FaultConfig(
            id="B1_pool_agotado",
            family=FaultFamily.DATABASE,
            fault_type=FaultType.DB_POOL_EXHAUSTION,
            enabled=False,
            description="Simulación de saturación del pool de conexiones PostgreSQL con rechazo OperationalError.",
            targets=["*"],
            traffic_pct=0.3,
            seed=42,
            parameters={
                "rejection_probability": 0.4,
                "error_message": "FATAL: remaining connection slots are reserved for non-replication superuser connections",
            },
        ),
        "B2_bloqueo_recurso": FaultConfig(
            id="B2_bloqueo_recurso",
            family=FaultFamily.DATABASE,
            fault_type=FaultType.RESOURCE_LOCK,
            enabled=False,
            description="Simulación de contención y espera por bloqueo exclusivo sobre tupla o recurso caliente (proveedor_id=3).",
            targets=["Q4", "Q5", "proveedor_3"],
            traffic_pct=1.0,
            seed=42,
            parameters={
                "lock_wait_ms": 80.0,
                "target_key": "proveedor_3",
            },
        ),

        # Familia C: Degradación de tiempos
        "C1_saturacion_cpu": FaultConfig(
            id="C1_saturacion_cpu",
            family=FaultFamily.DEGRADATION,
            fault_type=FaultType.CPU_BURN,
            enabled=False,
            description="Saturación controlada de cómputo en segundo plano para inducir contención de CPU.",
            targets=["*"],
            traffic_pct=1.0,
            seed=42,
            parameters={
                "intensity_pct": 60.0,
                "duration_s": 5.0,
                "threads": 2,
            },
        ),
        "C2_presion_memoria": FaultConfig(
            id="C2_presion_memoria",
            family=FaultFamily.DEGRADATION,
            fault_type=FaultType.MEMORY_PRESSURE,
            enabled=False,
            description="Presión transitoria de memoria RAM asignando búferes acotados con liberación garantizada.",
            targets=["*"],
            traffic_pct=1.0,
            seed=42,
            parameters={
                "size_mb": 150.0,
                "duration_s": 4.0,
            },
        ),

        # Familia D: Riesgo de disponibilidad
        "D1_particion_red": FaultConfig(
            id="D1_particion_red",
            family=FaultFamily.AVAILABILITY,
            fault_type=FaultType.NETWORK_PARTITION,
            enabled=False,
            description="Simulación de partición o corte de red transitorio hacia PostgreSQL o servicios externos.",
            targets=["*"],
            traffic_pct=1.0,
            seed=42,
            parameters={
                "duration_s": 3.0,
            },
        ),
        "D2_respuestas_5xx": FaultConfig(
            id="D2_respuestas_5xx",
            family=FaultFamily.AVAILABILITY,
            fault_type=FaultType.EXTERNAL_5XX,
            enabled=False,
            description="Inyección controlada de errores 5xx (503 Service Unavailable / 500) sobre porcentaje de tráfico.",
            targets=["POST /ordenes-compra", "Q1", "Q3"],
            traffic_pct=0.25,
            seed=42,
            parameters={
                "status_code": 503,
                "message": "Simulated upstream 503 Service Unavailable",
            },
        ),
    }


class ChaosConfigManager:
    """Administrador thread-safe con recarga en caliente vía mtime."""

    def __init__(self, config_path: Path = DEFAULT_CONFIG_PATH):
        self.config_path = Path(config_path)
        self._lock = threading.RLock()
        self._last_mtime: Optional[float] = None
        self._cached_config: Optional[ChaosGlobalConfig] = None
        self._ultima_revision: float = 0.0
        self._ensure_config_exists()

    def _ensure_config_exists(self) -> None:
        """Si el archivo no existe, lo inicializa con defaults seguros."""
        with self._lock:
            if not self.config_path.exists():
                cfg = ChaosGlobalConfig(
                    kill_switch=False,
                    enabled=False,
                    faults=_crear_catalogo_predeterminado(),
                    metadata={
                        "version": "1.0.0",
                        "project": "SupplyGrid",
                        "deliverable": "Entregable 3 · EAFIT",
                    },
                )
                self._save_to_disk(cfg)

    def _serialize_config(self, cfg: ChaosGlobalConfig) -> Dict[str, Any]:
        return {
            "kill_switch": cfg.kill_switch,
            "enabled": cfg.enabled,
            "metadata": cfg.metadata,
            "faults": {
                fid: {
                    "id": f.id,
                    "family": f.family.value,
                    "fault_type": f.fault_type.value,
                    "enabled": f.enabled,
                    "description": f.description,
                    "targets": f.targets,
                    "traffic_pct": f.traffic_pct,
                    "seed": f.seed,
                    "parameters": f.parameters,
                }
                for fid, f in cfg.faults.items()
            },
        }

    def _deserialize_config(self, data: Dict[str, Any]) -> ChaosGlobalConfig:
        faults_dict = {}
        for fid, f_data in data.get("faults", {}).items():
            try:
                faults_dict[fid] = FaultConfig(
                    id=f_data["id"],
                    family=FaultFamily(f_data["family"]),
                    fault_type=FaultType(f_data["fault_type"]),
                    enabled=bool(f_data.get("enabled", False)),
                    description=f_data.get("description", ""),
                    targets=f_data.get("targets", ["*"]),
                    traffic_pct=float(f_data.get("traffic_pct", 1.0)),
                    seed=f_data.get("seed", 42),
                    parameters=f_data.get("parameters", {}),
                )
            except Exception:
                # Si una falla es corrupta, no bloquea el resto
                continue

        return ChaosGlobalConfig(
            kill_switch=bool(data.get("kill_switch", False)),
            enabled=bool(data.get("enabled", False)),
            faults=faults_dict,
            metadata=data.get("metadata", {}),
        )

    def _save_to_disk(self, cfg: ChaosGlobalConfig) -> None:
        raw = self._serialize_config(cfg)
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.config_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2, ensure_ascii=False)
        temp_path.replace(self.config_path)
        self._last_mtime = self.config_path.stat().st_mtime
        self._cached_config = cfg

    def get_config(self) -> ChaosGlobalConfig:
        """Obtiene la configuración actual. Recarga automáticamente si el archivo fue modificado."""
        with self._lock:
            ahora = time.monotonic()
            if (self._cached_config is not None
                    and ahora - self._ultima_revision < INTERVALO_REVISION_S):
                return self._cached_config
            self._ultima_revision = ahora

            if not self.config_path.exists():
                self._ensure_config_exists()

            try:
                current_mtime = self.config_path.stat().st_mtime
            except OSError:
                current_mtime = 0.0

            if self._cached_config is None or self._last_mtime != current_mtime:
                try:
                    with open(self.config_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    cfg = self._deserialize_config(data)
                    self._cached_config = cfg
                    self._last_mtime = current_mtime
                except Exception as exc:
                    # Ante error de lectura o JSON corrupto, fallback seguro a no-op (desactivado)
                    if self._cached_config is None:
                        self._cached_config = ChaosGlobalConfig(
                            kill_switch=False,
                            enabled=False,
                            faults=_crear_catalogo_predeterminado(),
                            metadata={"error_fallback": str(exc)},
                        )

            return self._cached_config

    def set_kill_switch(self, active: bool) -> None:
        """Activa o desactiva el kill switch global de inmediato."""
        with self._lock:
            cfg = self.get_config()
            cfg.kill_switch = active
            self._save_to_disk(cfg)

    def set_global_enabled(self, enabled: bool) -> None:
        """Habilita o deshabilita el motor global de fallas."""
        with self._lock:
            cfg = self.get_config()
            cfg.enabled = enabled
            self._save_to_disk(cfg)

    def set_fault_state(self, fault_id: str, enabled: bool, parameters: Optional[Dict[str, Any]] = None, traffic_pct: Optional[float] = None) -> bool:
        """Habilita o deshabilita una falla específica, actualizando sus parámetros si se indican."""
        with self._lock:
            cfg = self.get_config()
            if fault_id not in cfg.faults:
                return False
            cfg.faults[fault_id].enabled = enabled
            if traffic_pct is not None:
                cfg.faults[fault_id].traffic_pct = float(traffic_pct)
            if parameters:
                cfg.faults[fault_id].parameters.update(parameters)
            # Si se activa una falla, también aseguramos que el motor global esté habilitado
            if enabled:
                cfg.enabled = True
            self._save_to_disk(cfg)
            return True

    def reset_defaults(self) -> None:
        """Restaura la configuración a su estado seguro predeterminado (todo apagado)."""
        with self._lock:
            cfg = ChaosGlobalConfig(
                kill_switch=False,
                enabled=False,
                faults=_crear_catalogo_predeterminado(),
                metadata={"reset": True, "project": "SupplyGrid"},
            )
            self._save_to_disk(cfg)
