"""
Reconstruye la línea de tiempo de experimentos/logs/eventos.jsonl.

Clasifica señales útiles (localizan la falla) frente a señales que
aportan poco o generan ruido. Salida:

    experimentos/logs/resumen_observabilidad.json
    experimentos/logs/resumen_observabilidad.md
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG = Path(__file__).resolve().parent / "logs" / "eventos.jsonl"
SALIDA_JSON = Path(__file__).resolve().parent / "logs" / "resumen_observabilidad.json"
SALIDA_MD = Path(__file__).resolve().parent / "logs" / "resumen_observabilidad.md"

UTILES = {
    "inyeccion": "Localiza la falla (fault_id + target) en el instante en que se aplicó.",
    "operacion": "Une el error observado (tipo, duración, target) con el trace_id.",
    "reintento": "Muestra si la corrección actuó y cuántas veces.",
    "circuito": "Explica rechazos rápidos cuando el error deja de ser transitorio.",
    "falla_activada": "Marca el inicio controlado de la perturbación.",
    "falla_desactivada": "Marca la reversión; permite comprobar el kill/reset.",
    "kill_switch": "Confirma que la prioridad absoluta quedó registrada.",
    "fase_inicio": "Delimita control / falla / corrección para comparar.",
    "fase_fin": "Cierra la ventana de medición con duración y recursos.",
}

POCO_UTILES = {
    "obs_iniciada": "Solo confirma que el canal existe; no localiza una falla.",
    "cache_hit": "En modo 'todas' inunda el log y no distingue causa.",
    "cache_miss": "Igual que cache_hit: el hit_rate consolidado basta.",
    "perturbaciones_detenidas": "Aparece al limpiar hilos; no implica kill switch real.",
}


def leer_eventos(ruta=LOG):
    eventos = []
    if not Path(ruta).exists():
        return eventos
    with Path(ruta).open(encoding="utf-8") as fh:
        for linea in fh:
            linea = linea.strip()
            if not linea:
                continue
            try:
                eventos.append(json.loads(linea))
            except json.JSONDecodeError:
                continue
    return eventos


def correlacionar(eventos):
    """Agrupa inyecciones y errores de operación por trace_id."""
    por_trace = defaultdict(list)
    for ev in eventos:
        tid = ev.get("trace_id")
        if tid:
            por_trace[tid].append(ev)

    cadenas = []
    for tid, sucesos in por_trace.items():
        tipos = [s.get("evento") for s in sucesos]
        if "inyeccion" in tipos and "operacion" in tipos:
            iny = next(s for s in sucesos if s["evento"] == "inyeccion")
            op = next(s for s in sucesos if s["evento"] == "operacion")
            cadenas.append({
                "trace_id": tid,
                "fault_id": iny.get("fault_id"),
                "target": iny.get("target") or op.get("target"),
                "error": op.get("error"),
                "experimento": op.get("experimento"),
                "fase": op.get("fase"),
            })
    return cadenas


def resumir(eventos):
    conteo = Counter(ev.get("evento") for ev in eventos)
    fallas = Counter(
        ev.get("fault_id")
        for ev in eventos
        if ev.get("evento") == "inyeccion"
    )
    errores = Counter(
        ev.get("error")
        for ev in eventos
        if ev.get("evento") == "operacion" and ev.get("resultado") == "error"
    )
    por_fase = Counter(
        (ev.get("experimento"), ev.get("fase"), ev.get("evento"))
        for ev in eventos
        if ev.get("experimento") and ev.get("fase")
    )
    cadenas = correlacionar(eventos)
    return {
        "total_eventos": len(eventos),
        "por_tipo": dict(conteo),
        "inyecciones_por_falla": dict(fallas),
        "errores_por_tipo": dict(errores),
        "cadenas_inyeccion_error": len(cadenas),
        "ejemplos_localizacion": cadenas[:8],
        "eventos_por_experimento_fase": {
            f"{e}|{f}|{ev}": n for (e, f, ev), n in por_fase.items()
        },
        "senales_utiles": UTILES,
        "senales_poco_utiles": POCO_UTILES,
        "criterio": (
            "Una señal es útil si permite detectar el síntoma, localizar "
            "la falla (fault_id/target/fase) y explicar la corrección. "
            "Es poco útil si no distingue causa o si su volumen oculta "
            "la secuencia causal."
        ),
    }


def markdown(resumen):
    lineas = [
        "# Resumen de observabilidad",
        "",
        f"Eventos totales: **{resumen['total_eventos']}**.",
        f"Cadenas inyección→error correlacionadas por `trace_id`: "
        f"**{resumen['cadenas_inyeccion_error']}**.",
        "",
        "## Conteos",
        "",
        "| Evento | Cantidad |",
        "|---|---:|",
    ]
    for k, v in sorted(resumen["por_tipo"].items(), key=lambda x: (-x[1], x[0])):
        lineas.append(f"| `{k}` | {v} |")

    lineas += [
        "",
        "## Inyecciones por falla",
        "",
        "| Falla | Inyecciones |",
        "|---|---:|",
    ]
    for k, v in sorted(resumen["inyecciones_por_falla"].items()):
        lineas.append(f"| `{k}` | {v} |")

    lineas += [
        "",
        "## Ejemplos de localización (trace_id)",
        "",
        "| trace_id | fault_id | error | experimento | fase |",
        "|---|---|---|---|---|",
    ]
    for ej in resumen["ejemplos_localizacion"]:
        lineas.append(
            f"| `{ej['trace_id'][:12]}…` | `{ej.get('fault_id')}` | "
            f"`{ej.get('error')}` | {ej.get('experimento')} | {ej.get('fase')} |"
        )

    lineas += [
        "",
        "## Señales útiles",
        "",
    ]
    for k, v in resumen["senales_utiles"].items():
        lineas.append(f"- `{k}`: {v}")

    lineas += [
        "",
        "## Señales poco útiles o ruidosas",
        "",
    ]
    for k, v in resumen["senales_poco_utiles"].items():
        lineas.append(f"- `{k}`: {v}")

    lineas += ["", resumen["criterio"], ""]
    return "\n".join(lineas)


def ejecutar(ruta_log=None):
    eventos = leer_eventos(ruta_log or LOG)
    resumen = resumir(eventos)
    SALIDA_JSON.parent.mkdir(parents=True, exist_ok=True)
    SALIDA_JSON.write_text(
        json.dumps(resumen, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    SALIDA_MD.write_text(markdown(resumen), encoding="utf-8")
    return resumen


if __name__ == "__main__":
    r = ejecutar()
    print(f"Eventos: {r['total_eventos']}")
    print(f"Cadenas localizadas: {r['cadenas_inyeccion_error']}")
    print(f"Escrito: {SALIDA_JSON}")
    print(f"Escrito: {SALIDA_MD}")
