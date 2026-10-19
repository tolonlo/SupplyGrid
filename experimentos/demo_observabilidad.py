"""
Demo corto de observabilidad (Persona B).

No sustituye los runners oficiales ni reescribe resultados de EXP-01..04.
Genera evidencia de: detectar, localizar y explicar; y compara dos
correcciones (reintentos vs circuit breaker) sobre la misma falla B1.

No requiere PostgreSQL: opera sobre funciones dummy interceptadas
por ChaosEngine, con configuración temporal.
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path

from chaos.config import ChaosConfigManager
from chaos.engine import ChaosDatabaseError, ChaosEngine, ChaosHttpError
from chaos import observabilidad as obs
from chaos.interceptor import with_chaos
from experimentos.analizar_observabilidad import ejecutar as analizar
from experimentos.mitigaciones import CacheTTL, CircuitBreaker, CircuitoAbierto


ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "experimentos" / "logs" / "eventos.jsonl"
OPS = 40


def _activar(manager, fault_id, params=None, traffic_pct=1.0):
    obs.falla(fault_id, True)
    manager.set_fault_state(
        fault_id, True, parameters=params, traffic_pct=traffic_pct,
    )


def _desactivar(manager, fault_id):
    obs.falla(fault_id, False)
    manager.set_fault_state(fault_id, False)


@with_chaos("Q1")
def consulta_q1():
    return "ok"


def _medir(n, recuperador):
    ok = errores = 0
    t0 = time.perf_counter()
    for _ in range(n):
        try:
            recuperador()
            ok += 1
        except Exception:
            errores += 1
    dur = time.perf_counter() - t0
    return {
        "ok": ok,
        "errores": errores,
        "disponibilidad_pct": 100.0 * ok / n if n else 0,
        "duracion_s": round(dur, 6),
    }


def _reintentos(max_intentos=3):
    def _fn():
        ultimo = None
        for intento in range(1, max_intentos + 1):
            try:
                return consulta_q1()
            except ChaosDatabaseError as exc:
                ultimo = exc
                if intento < max_intentos:
                    obs.reintento("Q1", intento, type(exc).__name__)
                    time.sleep(0.002 * (2 ** (intento - 1)))
        raise ultimo
    return _fn


def correr():
    LOG.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="chaos-obs-"))
    cfg_path = tmp / "chaos_config.json"
    manager = ChaosConfigManager(cfg_path)
    engine = ChaosEngine(manager)

    obs.reiniciar_para_pruebas(str(LOG))
    obs.iniciar()

    resultados = {}

    # EXP-01 sintetico: latencia + cache (la cache evita entrar al interceptor)
    cache = CacheTTL(ttl_segundos=60, capacidad=100, nombre="Q1")
    obs.abrir_fase("DEMO-01", "control")
    resultados["demo01_control"] = _medir(OPS, consulta_q1)
    obs.cerrar_fase()

    _activar(manager, "A1_latencia_jitter", {"mean_ms": 15.0, "jitter_ms": 2.0}, 1.0)
    try:
        obs.abrir_fase("DEMO-01", "con_falla")
        resultados["demo01_falla"] = _medir(OPS, consulta_q1)
        obs.cerrar_fase()

        obs.abrir_fase("DEMO-01", "con_correccion")
        def con_cache():
            hit = cache.obtener("k")
            if hit is not None:
                return hit
            val = consulta_q1()
            cache.guardar("k", val)
            return val
        resultados["demo01_correccion"] = _medir(OPS, con_cache)
        resultados["demo01_cache"] = cache.estadisticas()
        obs.cerrar_fase()
    finally:
        _desactivar(manager, "A1_latencia_jitter")

    # EXP-02 sintetico: B1 + reintentos vs circuit breaker
    _activar(
        manager, "B1_pool_agotado",
        {"rejection_probability": 1.0}, traffic_pct=0.4,
    )
    try:
        obs.abrir_fase("DEMO-02", "con_falla")
        resultados["demo02_falla"] = _medir(OPS, consulta_q1)
        obs.cerrar_fase()

        obs.abrir_fase("DEMO-02", "reintentos")
        resultados["demo02_reintentos"] = _medir(OPS, _reintentos(3))
        obs.cerrar_fase()

        breaker = CircuitBreaker(umbral_fallos=4, timeout_abierto_s=0.05, nombre="Q1")
        obs.abrir_fase("DEMO-02", "circuito")

        def con_circuito():
            try:
                return breaker.ejecutar(consulta_q1)
            except CircuitoAbierto:
                raise
        resultados["demo02_circuito"] = _medir(OPS, con_circuito)
        resultados["demo02_circuito_stats"] = breaker.estadisticas()
        obs.cerrar_fase()
    finally:
        _desactivar(manager, "B1_pool_agotado")

    # Kill switch: la falla queda ON en config pero no se inyecta
    _activar(manager, "D2_respuestas_5xx", {"status_code": 503}, 1.0)
    manager.set_kill_switch(True)
    obs.kill_switch("demo")
    engine.stop_all_disturbances()
    obs.abrir_fase("DEMO-KS", "kill_switch")
    ks_errores = 0
    for _ in range(10):
        try:
            consulta_q1()
        except ChaosHttpError:
            ks_errores += 1
    resultados["demo_kill_switch_errores"] = ks_errores
    obs.cerrar_fase()
    manager.set_kill_switch(False)
    _desactivar(manager, "D2_respuestas_5xx")
    manager.reset_defaults()

    resumen = analizar(LOG)
    shutil.rmtree(tmp, ignore_errors=True)
    return resultados, resumen


if __name__ == "__main__":
    res, resumen = correr()
    print("Resultados demo:")
    for k, v in res.items():
        print(f"  {k}: {v}")
    print(f"Eventos en log: {resumen['total_eventos']}")
    print(f"Cadenas localizadas: {resumen['cadenas_inyeccion_error']}")
