"""
Observabilidad de SupplyGrid (Persona B · E3).

Tres señales; no se exige una herramienta concreta:
  - LOGS     experimentos/logs/eventos.jsonl  (obligatorios)
  - METRICAS Prometheus en :OBS_METRICS_PORT/metrics (si hay prometheus_client)
  - TRAZAS   correlation-id en cada log; OTLP/Jaeger solo si hay endpoint

Cada operación interceptada lleva un trace_id compartido con inyecciones,
reintentos y el circuit breaker. Eso permite localizar la falla que causó
un error sin volcar cada operación exitosa.

Variables:
  OBS_LOG_PATH                 por defecto experimentos/logs/eventos.jsonl
  OBS_LOG_OPS                  errores | todas | no
  OBS_METRICS_PORT             0 = desactivado
  OTEL_EXPORTER_OTLP_ENDPOINT  p. ej. http://localhost:4318
"""

from __future__ import annotations

import atexit
import contextlib
import contextvars
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone

RUTA_LOG = os.environ.get("OBS_LOG_PATH", "experimentos/logs/eventos.jsonl")
MODO_OPS = os.environ.get("OBS_LOG_OPS", "errores").lower()

_lock = threading.Lock()
_ctx = contextvars.ContextVar("obs_ctx", default={})
_fh = None
_pendientes = 0
_tracer = None
_M = {}
_iniciada = False
_pila_fases = []
_atexit_ok = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    _PROM = True
except ImportError:  # pragma: no cover
    _PROM = False

try:
    from opentelemetry import trace as _otel_trace
    _OTEL = True
except ImportError:  # pragma: no cover
    _OTEL = False


def _abrir_log():
    global _fh, _atexit_ok
    if _fh is None:
        os.makedirs(os.path.dirname(RUTA_LOG) or ".", exist_ok=True)
        _fh = open(RUTA_LOG, "a", encoding="utf-8", buffering=1 << 16)
        if not _atexit_ok:
            atexit.register(vaciar)
            _atexit_ok = True


def vaciar():
    with _lock:
        if _fh is not None:
            _fh.flush()


def _recursos():
    try:
        import psutil
        proc = psutil.Process()
        mem = proc.memory_info()
        return {
            "rss_mb": round(mem.rss / (1024 * 1024), 3),
            "cpu_proc_pct": proc.cpu_percent(interval=None),
        }
    except Exception:
        return {}


def _ctx_actual():
    return dict(_ctx.get() or {})


def evento(nombre, **campos):
    """Escribe un evento JSON con marca de tiempo y contexto (experimento, fase, trace_id)."""
    global _pendientes
    reg = {"ts": datetime.now(timezone.utc).isoformat(), "evento": nombre, **_ctx_actual(), **campos}
    linea = json.dumps(reg, ensure_ascii=False, default=str) + "\n"
    with _lock:
        _abrir_log()
        _fh.write(linea)
        _pendientes += 1
        if _pendientes >= 200:
            _fh.flush()
            _pendientes = 0


def iniciar(puerto_metricas=None, otlp_endpoint=None, forzar=False):
    """Arranca métricas y trazas si hay librerías. Los logs siempre quedan listos."""
    global _tracer, _iniciada
    if _iniciada and not forzar:
        return
    _iniciada = True
    _abrir_log()

    if _PROM and not _M:
        _M["dur"] = Histogram(
            "chaos_op_duracion_seconds",
            "Duración de operaciones interceptadas",
            ["target", "experimento", "fase", "resultado"],
            buckets=(.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 5),
        )
        _M["ops"] = Counter(
            "chaos_op_total", "Operaciones interceptadas",
            ["target", "experimento", "fase", "resultado", "error"],
        )
        _M["retry"] = Counter(
            "chaos_reintentos_total", "Reintentos de mitigación",
            ["target", "experimento", "causa"],
        )
        _M["cache"] = Counter(
            "chaos_cache_total", "Aciertos y fallos de caché",
            ["target", "resultado"],
        )
        _M["inyec"] = Counter(
            "chaos_inyecciones_total", "Fallas realmente inyectadas",
            ["fault_id", "target"],
        )
        _M["falla"] = Gauge("chaos_falla_activa", "1 si la falla está activa", ["fault_id"])
        _M["circuito"] = Counter(
            "chaos_circuito_total", "Cambios de estado del circuit breaker",
            ["target", "estado"],
        )
        _M["rss"] = Gauge("chaos_rss_bytes", "RSS del proceso Python")
        puerto = puerto_metricas if puerto_metricas is not None else int(os.environ.get("OBS_METRICS_PORT", "0"))
        if puerto:
            start_http_server(puerto)

    endpoint = otlp_endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if _OTEL and endpoint and _tracer is None:
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        prov = TracerProvider(resource=Resource.create({"service.name": "supplygrid-chaos"}))
        prov.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces"))
        )
        _otel_trace.set_tracer_provider(prov)
        _tracer = _otel_trace.get_tracer("supplygrid.chaos")

    evento(
        "obs_iniciada",
        metricas=bool(_M),
        trazas_otlp=_tracer is not None,
        trazas_locales=True,
        modo_ops=MODO_OPS,
    )


def contexto(experimento, fase):
    """Marca las operaciones de la fase con experimento y fase; registra inicio y fin."""
    @contextlib.contextmanager
    def _cm():
        abrir_fase(experimento, fase)
        try:
            yield
        finally:
            cerrar_fase()
    return _cm()


def abrir_fase(experimento, fase):
    """Versión sin 'with' de contexto(): se cierra con cerrar_fase()."""
    previo = _ctx_actual()
    token = _ctx.set({"experimento": experimento, "fase": fase})
    _pila_fases.append((token, time.perf_counter(), previo))
    evento("fase_inicio", **_recursos())


def cerrar_fase():
    if not _pila_fases:
        return
    token, t0, _previo = _pila_fases.pop()
    evento("fase_fin", duracion_s=round(time.perf_counter() - t0, 6), **_recursos())
    vaciar()
    _ctx.reset(token)


def falla(fault_id, activa):
    """Activación o desactivación de una falla (incluye el kill switch)."""
    if _M:
        _M["falla"].labels(fault_id).set(1 if activa else 0)
    evento("falla_activada" if activa else "falla_desactivada", fault_id=fault_id)


def inyeccion(fault, target, **extra):
    """Se llama cuando una falla realmente se aplicó a una operación."""
    fault_id = getattr(fault, "id", None) or str(getattr(getattr(fault, "fault_type", ""), "value", ""))
    if _M:
        _M["inyec"].labels(str(fault_id), str(target)).inc()
    evento("inyeccion", fault_id=fault_id, target=target, **extra)


def kill_switch(motivo="stop_all_disturbances"):
    evento("kill_switch", motivo=motivo)
    vaciar()


def reintento(target, intento, causa):
    if _M:
        _M["retry"].labels(target, _ctx_actual().get("experimento", "-"), causa).inc()
    evento("reintento", target=target, intento=intento, causa=causa)


def cache(target, hit):
    if _M:
        _M["cache"].labels(target, "hit" if hit else "miss").inc()
    if MODO_OPS == "todas":
        evento("cache_hit" if hit else "cache_miss", target=target)


def circuito(target, estado, motivo=""):
    if _M:
        _M["circuito"].labels(target, estado).inc()
    evento("circuito", target=target, estado=estado, motivo=motivo or None)


def _trace_id_otel(span):
    if span is not None:
        c = span.get_span_context()
        if c.is_valid:
            return format(c.trace_id, "032x")
    return None


def observar_operacion(target, fn):
    """Ejecuta fn() midiendo duración y resultado. Relanza la excepción original."""
    base = _ctx_actual()
    exp, fase = base.get("experimento", "-"), base.get("fase", "-")
    cm = _tracer.start_as_current_span(f"op {target}") if _tracer else contextlib.nullcontext()
    t0 = time.perf_counter()
    with cm as span:
        tid = _trace_id_otel(span) or uuid.uuid4().hex
        token = _ctx.set({**base, "trace_id": tid, "target": target})
        if span is not None:
            span.set_attribute("chaos.experimento", exp)
            span.set_attribute("chaos.fase", fase)
            span.set_attribute("chaos.target", target)
        try:
            try:
                res = fn()
            except Exception as exc:
                _registrar(target, exp, fase, t0, "error", type(exc).__name__, tid, str(exc)[:200])
                raise
            _registrar(target, exp, fase, t0, "ok", "", tid, "")
            return res
        finally:
            _ctx.reset(token)


def _registrar(target, exp, fase, t0, resultado, error, tid, detalle):
    dur = time.perf_counter() - t0
    if _M:
        _M["dur"].labels(target, exp, fase, resultado).observe(dur)
        _M["ops"].labels(target, exp, fase, resultado, error).inc()
        rss = _recursos().get("rss_mb")
        if rss is not None and "rss" in _M:
            _M["rss"].set(rss * 1024 * 1024)
    if MODO_OPS == "no":
        return
    if resultado == "ok" and MODO_OPS != "todas":
        return
    evento(
        "operacion",
        target=target,
        resultado=resultado,
        error=error or None,
        dur_ms=round(dur * 1000, 4),
        trace_id=tid,
        detalle=detalle or None,
    )


def reiniciar_para_pruebas(ruta_log=None):
    """Cierra el log y permite volver a iniciar() en tests. No usar en producción."""
    global _iniciada, _fh, _pendientes, RUTA_LOG
    with _lock:
        if _fh is not None:
            _fh.flush()
            _fh.close()
            _fh = None
        _pendientes = 0
        _iniciada = False
        while _pila_fases:
            token, _, _ = _pila_fases.pop()
            try:
                _ctx.reset(token)
            except Exception:
                pass
    if ruta_log:
        RUTA_LOG = ruta_log
