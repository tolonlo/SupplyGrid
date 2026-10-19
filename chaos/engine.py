"""
Motor de ejecución de fallas configurables de SupplyGrid (ChaosEngine).
Implementa las 4 familias de fallas con focalización, parametrización,
reproducibilidad y reversibilidad inmediata mediante Kill Switch.
"""

import math
import random
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import ChaosConfigManager
from .models import FaultConfig, FaultFamily, FaultType


class ChaosException(Exception):
    """Excepción base para fallas inyectadas."""
    pass


class ChaosDatabaseError(ChaosException):
    """Simula fallas de base de datos (Familia B)."""
    pass


class ChaosTimeoutError(ChaosException):
    """Simula timeout controlado (Familia A)."""
    pass


class ChaosHttpError(ChaosException):
    """Simula error HTTP 5xx o desconexión (Familia D)."""
    def __init__(self, status_code: int, message: str):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class ChaosEngine:
    """Motor central de inyección de perturbaciones."""

    _instance = None
    _singleton_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super(ChaosEngine, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, config_manager: Optional[ChaosConfigManager] = None):
        if config_manager is not None:
            self.config_manager = config_manager
        if self._initialized:
            return
        if not hasattr(self, "config_manager"):
            self.config_manager = config_manager or ChaosConfigManager()
        self._active_burn_threads: List[threading.Thread] = []
        self._stop_events: List[threading.Event] = []
        self._memory_buffers: List[bytearray] = []
        self._lock = threading.RLock()
        self._initialized = True

    def is_kill_switch_active(self) -> bool:
        """Comprueba de forma prioritaria si el Kill Switch global está activado."""
        cfg = self.config_manager.get_config()
        return cfg.kill_switch

    def stop_all_disturbances(self) -> None:
        """Detiene de inmediato cualquier perturbación activa en curso (CPU, memoria, delays)."""
        with self._lock:
            for ev in self._stop_events:
                ev.set()
            self._stop_events.clear()
            self._active_burn_threads.clear()
            self._memory_buffers.clear()

    def should_inject(self, fault: FaultConfig, target: str, execution_id: Optional[int] = None) -> bool:
        """Determina si la falla debe aplicarse respetando focalización, tráfico y semilla."""
        if self.is_kill_switch_active():
            return False

        cfg = self.config_manager.get_config()
        if not cfg.enabled or not fault.enabled:
            return False

        # 1. Comprobar focalización (target)
        if "*" not in fault.targets and target not in fault.targets:
            # Comprobación de prefijo/substring para endpoints
            matches = any(t in target or target in t for t in fault.targets)
            if not matches:
                return False

        # 2. Comprobar porcentaje de tráfico (con semilla reproducible si existe)
        if fault.traffic_pct < 1.0:
            if fault.seed is not None and execution_id is not None:
                # Determinista por ejecución
                rng = random.Random(fault.seed + int(execution_id))
            else:
                rng = random.Random()
            if rng.random() > fault.traffic_pct:
                return False

        return True

    def apply_latency_jitter(self, fault: FaultConfig, target: str, execution_id: Optional[int] = None) -> float:
        """Familia A1: Inyecta latencia fija con jitter lognormal o gaussiano."""
        if not self.should_inject(fault, target, execution_id):
            return 0.0

        params = fault.parameters
        mean_ms = float(params.get("mean_ms", 25.0))
        jitter_ms = float(params.get("jitter_ms", 5.0))
        dist = params.get("distribution", "lognormal")

        if fault.seed is not None and execution_id is not None:
            rng = random.Random(fault.seed + int(execution_id))
        else:
            rng = random.Random()

        if dist == "lognormal" and jitter_ms > 0:
            # Aproximación lognormal centrada en mean_ms
            variance = jitter_ms ** 2
            mu = math.log((mean_ms ** 2) / math.sqrt(variance + mean_ms ** 2))
            sigma = math.sqrt(math.log(1.0 + (variance / (mean_ms ** 2))))
            delay_ms = rng.lognormvariate(mu, sigma)
        else:
            delay_ms = max(0.0, rng.gauss(mean_ms, jitter_ms))

        # Sleep reversible: revisa kill_switch cada 10ms
        delay_s = delay_ms / 1000.0
        elapsed = 0.0
        step = 0.010
        while elapsed < delay_s:
            if self.is_kill_switch_active():
                return elapsed * 1000.0
            sleep_chunk = min(step, delay_s - elapsed)
            time.sleep(sleep_chunk)
            elapsed += sleep_chunk

        return elapsed * 1000.0

    def check_controlled_timeout(self, fault: FaultConfig, elapsed_ms: float, target: str, execution_id: Optional[int] = None) -> None:
        """Familia A2: Lanza timeout si la operación excedió el límite configurado."""
        if not self.should_inject(fault, target, execution_id):
            return

        timeout_ms = float(fault.parameters.get("timeout_ms", 15.0))
        if elapsed_ms > timeout_ms:
            raise ChaosTimeoutError(
                f"[Falla A2] Timeout controlado excedido en target '{target}': "
                f"{elapsed_ms:.2f} ms > {timeout_ms:.2f} ms"
            )

    def maybe_simulate_db_failure(self, fault: FaultConfig, target: str, execution_id: Optional[int] = None) -> None:
        """Familia B1 y B2: Simula saturación de pool o bloqueo de recurso."""
        if not self.should_inject(fault, target, execution_id):
            return

        if fault.fault_type == FaultType.DB_POOL_EXHAUSTION:
            msg = fault.parameters.get(
                "error_message",
                "FATAL: remaining connection slots are reserved for non-replication superuser connections"
            )
            raise ChaosDatabaseError(f"[Falla B1] Pool de conexiones agotado: {msg}")

        elif fault.fault_type == FaultType.RESOURCE_LOCK:
            wait_ms = float(fault.parameters.get("lock_wait_ms", 80.0))
            # Simula tiempo de espera por lock
            delay_s = wait_ms / 1000.0
            elapsed = 0.0
            step = 0.010
            while elapsed < delay_s:
                if self.is_kill_switch_active():
                    return
                sleep_chunk = min(step, delay_s - elapsed)
                time.sleep(sleep_chunk)
                elapsed += sleep_chunk

    def start_cpu_burn(self, fault: FaultConfig) -> None:
        """Familia C1: Inicia hilos de cómputo intensivo para saturar CPU."""
        if self.is_kill_switch_active():
            return

        intensity = float(fault.parameters.get("intensity_pct", 60.0)) / 100.0
        duration_s = float(fault.parameters.get("duration_s", 5.0))
        n_threads = int(fault.parameters.get("threads", 2))

        stop_event = threading.Event()
        self._stop_events.append(stop_event)

        def _worker():
            start = time.time()
            work_chunk = 0.005 * intensity
            sleep_chunk = 0.005 * (1.0 - intensity)
            while not stop_event.is_set() and (time.time() - start) < duration_s:
                if self.is_kill_switch_active():
                    break
                # Cómputo sintético
                t0 = time.time()
                while (time.time() - t0) < work_chunk:
                    _ = [x * x for x in range(200)]
                if sleep_chunk > 0:
                    time.sleep(sleep_chunk)

        with self._lock:
            for _ in range(n_threads):
                t = threading.Thread(target=_worker, daemon=True)
                self._active_burn_threads.append(t)
                t.start()

    def apply_memory_pressure(self, fault: FaultConfig) -> None:
        """Familia C2: Asigna búferes de memoria controlados."""
        if self.is_kill_switch_active():
            return

        size_mb = float(fault.parameters.get("size_mb", 100.0))
        duration_s = float(fault.parameters.get("duration_s", 4.0))

        try:
            bytes_to_alloc = int(size_mb * 1024 * 1024)
            buf = bytearray(bytes_to_alloc)
            # Tocar memoria para forzar asignación en páginas físicas
            for i in range(0, min(len(buf), 1024 * 1024 * 10), 4096):
                buf[i] = 1
            with self._lock:
                self._memory_buffers.append(buf)

            # Programar liberación automática
            def _releaser():
                time.sleep(duration_s)
                with self._lock:
                    if buf in self._memory_buffers:
                        self._memory_buffers.remove(buf)

            threading.Thread(target=_releaser, daemon=True).start()
        except MemoryError:
            pass

    def check_availability_risk(self, fault: FaultConfig, target: str, execution_id: Optional[int] = None) -> None:
        """Familia D: Simula partición de red o error 5xx de servicio externo."""
        if not self.should_inject(fault, target, execution_id):
            return

        if fault.fault_type == FaultType.NETWORK_PARTITION:
            raise ChaosDatabaseError(
                "[Falla D1] Partición de red: connection to server was lost / socket closed"
            )

        elif fault.fault_type == FaultType.EXTERNAL_5XX:
            status = int(fault.parameters.get("status_code", 503))
            msg = fault.parameters.get("message", "Service Unavailable")
            raise ChaosHttpError(status, f"[Falla D2] {msg}")

    def intercept_operation(self, target: str, operation: Callable[..., Any], *args, **kwargs) -> Any:
        """Envuelve y ejecuta una operación aplicando el catálogo activo de fallas."""
        if self.is_kill_switch_active():
            return operation(*args, **kwargs)

        cfg = self.config_manager.get_config()
        if not cfg.enabled:
            return operation(*args, **kwargs)

        # 1. Comprobar disponibilidad de red / 5xx
        for fault in cfg.faults.values():
            if fault.family == FaultFamily.AVAILABILITY and fault.enabled:
                self.check_availability_risk(fault, target)

        # 2. Comprobar fallas de base de datos
        for fault in cfg.faults.values():
            if fault.family == FaultFamily.DATABASE and fault.enabled:
                self.maybe_simulate_db_failure(fault, target)

        # 3. Aplicar latencia inducida antes de la operación
        latency_injected = 0.0
        for fault in cfg.faults.values():
            if fault.fault_type == FaultType.LATENCY_JITTER and fault.enabled:
                latency_injected += self.apply_latency_jitter(fault, target)

        t_start = time.perf_counter()
        result = operation(*args, **kwargs)
        t_duration_ms = (time.perf_counter() - t_start) * 1000.0 + latency_injected

        # 4. Comprobar timeout controlado
        for fault in cfg.faults.values():
            if fault.fault_type == FaultType.CONTROLLED_TIMEOUT and fault.enabled:
                self.check_controlled_timeout(fault, t_duration_ms, target)

        return result
