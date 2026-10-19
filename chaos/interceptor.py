"""
Interceptores y decoradores para inyección no intrusiva de fallas en SupplyGrid.
Permite envolver conexiones de base de datos psycopg2 y llamadas de benchmarking.

E3: cada operación interceptada pasa por observabilidad.observar_operacion, que mide
duración y resultado y emite métricas, logs y trazas.
"""

import functools
import re
from typing import Any, Callable, Optional

from . import observabilidad as obs
from .engine import ChaosEngine


def _infer_target(sql: str) -> str:
    """Infiere el nombre de la consulta o tabla a partir del SQL."""
    sql_upper = sql.strip().upper()

    if "PROVEEDORES.CONTRATOS" in sql_upper and "FECHA_FIN" in sql_upper:
        return "Q1"
    elif "CATALOGO.CATALOGO_SKU" in sql_upper:
        return "Q2"
    elif "LOGISTICA.FRANJAS_DESCARGUE" in sql_upper:
        return "Q3"
    elif "DATE_TRUNC('MONTH'" in sql_upper or ("ORDENES.LINEAS_ORDEN" in sql_upper and "SUM" in sql_upper):
        return "Q4"
    elif "LIMIT 50" in sql_upper and "ORDENES.ORDENES" in sql_upper:
        return "Q5"

    # Match de tabla genérico
    m = re.search(r'(?:FROM|INTO|UPDATE)\s+([a-zA-Z0-9_\.]+)', sql, re.IGNORECASE)
    if m:
        return m.group(1).lower()

    return "sql_query"


class ChaosCursor:
    """Cursor que intercepta consultas para aplicar las fallas configuradas."""

    def __init__(self, real_cursor: Any, engine: Optional[ChaosEngine] = None):
        self._cursor = real_cursor
        self._engine = engine or ChaosEngine()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    def execute(self, sql: str, params: Any = None) -> Any:
        target = _infer_target(sql)
        # Si el parámetro incluye proveedor 3 (caso caliente), marcar el target específico
        if params and isinstance(params, (list, tuple)) and any(p == 3 for p in params):
            target_key = f"{target}:proveedor_3"
        else:
            target_key = target

        def _do_execute():
            return self._cursor.execute(sql, params)

        return obs.observar_operacion(
            target_key,
            lambda: self._engine.intercept_operation(target_key, _do_execute),
        )

    def executemany(self, sql: str, seq_of_params: Any) -> Any:
        target = _infer_target(sql)

        def _do_executemany():
            return self._cursor.executemany(sql, seq_of_params)

        return obs.observar_operacion(
            target,
            lambda: self._engine.intercept_operation(target, _do_executemany),
        )


class ChaosConnection:
    """Wrapper de conexión psycopg2 con soporte de inyección de fallas."""

    def __init__(self, real_conn: Any, engine: Optional[ChaosEngine] = None):
        self._conn = real_conn
        self._engine = engine or ChaosEngine()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def cursor(self, *args, **kwargs) -> ChaosCursor:
        cur = self._conn.cursor(*args, **kwargs)
        return ChaosCursor(cur, self._engine)

    def close(self) -> None:
        return self._conn.close()


def connect_with_chaos(*args, **kwargs) -> ChaosConnection:
    """Punto de entrada transparente para psycopg2.connect con inyección de fallas."""
    import psycopg2

    engine = ChaosEngine()
    # Si la falla D1 (partición de red) o B1 (pool agotado) está activa a nivel de conexión
    cfg = engine.config_manager.get_config()
    if not engine.is_kill_switch_active() and cfg.enabled:

        def _chequear_conexion():
            for fault in cfg.faults.values():
                if fault.enabled:
                    engine.check_availability_risk(fault, "connect")
                    engine.maybe_simulate_db_failure(fault, "connect")

        obs.observar_operacion("connect", _chequear_conexion)

    real_conn = psycopg2.connect(*args, **kwargs)
    return ChaosConnection(real_conn, engine)


def with_chaos(target_name: str):
    """Decorador para inyectar fallas sobre una función o endpoint."""
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            engine = ChaosEngine()
            return obs.observar_operacion(
                target_name,
                lambda: engine.intercept_operation(target_name, func, *args, **kwargs),
            )
        return wrapper
    return decorator
