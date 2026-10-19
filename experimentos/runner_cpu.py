"""
SupplyGrid - EXP-03
Familia C: Saturación controlada de CPU.

Diseño:
    - Control: sin perturbación.
    - Falla: carga CPU con intensidad configurada del 60 %.
    - Corrección: reducción de intensidad al 20 %.

Se realizan cinco repeticiones experimentales.

Las mediciones de CPU corresponden al proceso Python
y al sistema anfitrión. No representan directamente
el consumo del servidor PostgreSQL.

La corrección consiste en reducir la carga artificial,
no en modificar el motor de fallas.
"""

import json
import os
import statistics
import threading
import time

from datetime import datetime, timezone

import psutil

from chaos.config import ChaosConfigManager
from chaos.engine import ChaosEngine

from experimentos.runner import (
    ROOT,
    RESULTADOS,
    SQL_Q1,
    conectar,
    percentil,
    preparar_parametros_q1,
    checksum_parametros,
    resumen_estadistico,
)


EXPERIMENTO = {
    "id": "EXP-03",
    "familia": "C",
    "fault_id": "C1_saturacion_cpu",
    "consulta": "Q1",
    "escenario": "frio",
    "seed": 42,
    "hipotesis": (
        "La carga computacional artificial aumentará "
        "la utilización de CPU y podrá degradar "
        "los tiempos de respuesta. Reducir su intensidad "
        "disminuirá la presión sobre los recursos."
    ),
}

REPETICIONES_EXPERIMENTALES = 5

DURACION_FASE_S = 5.0

INTERVALO_MUESTREO_S = 0.20

INTENSIDAD_FALLA = 60.0

INTENSIDAD_CORRECCION = 20.0

HILOS_CPU = 2

PROCESO = psutil.Process(os.getpid())


def resumir_muestras(valores):
    """Resume las muestras de recursos obtenidas."""

    if not valores:
        return {
            "promedio": 0.0,
            "maximo": 0.0,
            "minimo": 0.0,
            "muestras": 0,
        }

    return {
        "promedio": statistics.mean(valores),
        "maximo": max(valores),
        "minimo": min(valores),
        "muestras": len(valores),
    }


def monitorizar_recursos(detener, almacenamiento):
    """
    Obtiene muestras de CPU y memoria.

    CPU del proceso puede superar 100 % cuando
    se utilizan varios núcleos.
    """

    cpu_proceso = []
    cpu_sistema = []
    memoria_mb = []

    PROCESO.cpu_percent(interval=None)
    psutil.cpu_percent(interval=None)

    while not detener.is_set():

        try:
            cpu_proceso.append(
                PROCESO.cpu_percent(interval=None)
            )

            cpu_sistema.append(
                psutil.cpu_percent(interval=None)
            )

            memoria_mb.append(
                PROCESO.memory_info().rss
                / (1024 * 1024)
            )

        except psutil.Error:
            pass

        detener.wait(INTERVALO_MUESTREO_S)

    almacenamiento["cpu_proceso_pct"] = (
        resumir_muestras(cpu_proceso)
    )

    almacenamiento["cpu_sistema_pct"] = (
        resumir_muestras(cpu_sistema)
    )

    almacenamiento["memoria_proceso_mb"] = (
        resumir_muestras(memoria_mb)
    )


def medir_q1_durante_ventana(
    conexion,
    parametros_q1,
    duracion_s
):
    """
    Ejecuta Q1 repetidamente durante una ventana temporal.

    Utiliza los mismos parámetros generados con seed 42.

    No utiliza caché para que cada operación realice SQL.
    """

    cursor = conexion.cursor()

    tiempos_ms = []

    errores = []

    total = 0

    exitosas = 0

    inicio = time.perf_counter()

    indice = 0

    while (
        time.perf_counter() - inicio
    ) < duracion_s:

        parametros = parametros_q1[
            indice % len(parametros_q1)
        ]

        indice += 1

        total += 1

        inicio_operacion = (
            time.perf_counter_ns()
        )

        try:

            cursor.execute(
                SQL_Q1,
                parametros
            )

            cursor.fetchall()

            exitosas += 1

        except Exception as error:

            errores.append({
                "tipo": type(error).__name__,
                "mensaje": str(error),
            })

        finalmente = (
            time.perf_counter_ns()
        )

        tiempos_ms.append(
            (
                finalmente
                - inicio_operacion
            ) / 1_000_000
        )

    duracion_real = (
        time.perf_counter()
        - inicio
    )

    return {
        "ejecuciones": total,
        "exitosas": exitosas,
        "errores": len(errores),
        "disponibilidad_pct": (
            exitosas / total * 100
            if total
            else 0.0
        ),
        "p50_ms": percentil(
            tiempos_ms,
            0.50
        ),
        "p95_ms": percentil(
            tiempos_ms,
            0.95
        ),
        "p99_ms": percentil(
            tiempos_ms,
            0.99
        ),
        "throughput_ops_s": (
            exitosas / duracion_real
            if duracion_real > 0
            else 0.0
        ),
        "duracion_total_s": duracion_real,
        "tiempos_crudos_ms": tiempos_ms,
        "detalle_errores": errores[:20],
    }


def ejecutar_fase(
    nombre,
    parametros_q1,
    manager,
    engine,
    intensidad=None
):
    """
    Ejecuta una fase con monitoreo de recursos.

    El motor de CPU se inicia explícitamente porque
    intercept_operation no activa C1 automáticamente.
    """

    print(f"\nFASE: {nombre}")

    conexion = conectar(
        con_chaos=True
    )

    detener_monitor = threading.Event()

    recursos = {}

    monitor = threading.Thread(
        target=monitorizar_recursos,
        args=(detener_monitor, recursos),
        daemon=True,
    )

    try:

        if intensidad is not None:

            manager.set_fault_state(
                EXPERIMENTO["fault_id"],
                True,
                parameters={
                    "intensity_pct": intensidad,
                    "duration_s": (
                        DURACION_FASE_S + 0.5
                    ),
                    "threads": HILOS_CPU,
                },
                traffic_pct=1.0,
            )

            configuracion = (
                manager.get_config()
            )

            falla = configuracion.faults[
                EXPERIMENTO["fault_id"]
            ]

            engine.start_cpu_burn(falla)

        monitor.start()

        resultado = (
            medir_q1_durante_ventana(
                conexion,
                parametros_q1,
                DURACION_FASE_S,
            )
        )

    finally:

        detener_monitor.set()

        if monitor.ident is not None:
            monitor.join(timeout=2.0)

        engine.stop_all_disturbances()

        manager.set_fault_state(
            EXPERIMENTO["fault_id"],
            False
        )

        conexion.close()

    resultado["recursos"] = recursos

    resultado["intensidad_configurada_pct"] = (
        intensidad if intensidad is not None else 0.0
    )

    resultado["hilos_cpu_configurados"] = (
        HILOS_CPU if intensidad is not None else 0
    )

    print(
        "  Operaciones:",
        resultado["ejecuciones"]
    )

    print(
        "  p95:",
        f"{resultado['p95_ms']:.4f} ms"
    )

    print(
        "  Throughput:",
        f"{resultado['throughput_ops_s']:.2f} ops/s"
    )

    print(
        "  CPU proceso promedio:",
        f"{recursos['cpu_proceso_pct']['promedio']:.2f}%"
    )

    print(
        "  CPU sistema promedio:",
        f"{recursos['cpu_sistema_pct']['promedio']:.2f}%"
    )

    return resultado


def ejecutar_repeticion(
    numero,
    parametros_q1,
    manager,
    engine,
    identificador
):

    print("\n" + "=" * 65)

    print(
        f"EXP-03 | REPETICIÓN "
        f"{numero}/{REPETICIONES_EXPERIMENTALES}"
    )

    print("=" * 65)

    control = ejecutar_fase(
        "control",
        parametros_q1,
        manager,
        engine,
        intensidad=None,
    )

    orden = (
        ["con_falla", "con_correccion"]
        if numero % 2
        else ["con_correccion", "con_falla"]
    )

    resultados = {}

    for fase in orden:

        intensidad = (
            INTENSIDAD_FALLA
            if fase == "con_falla"
            else INTENSIDAD_CORRECCION
        )

        resultados[fase] = ejecutar_fase(
            fase,
            parametros_q1,
            manager,
            engine,
            intensidad=intensidad,
        )

        time.sleep(0.5)

    resultado = {
        "experimento": EXPERIMENTO,
        "fecha": datetime.now(
            timezone.utc
        ).isoformat(),
        "numero_repeticion": numero,
        "identificador_ejecucion": identificador,
        "orden_fases": [
            "control",
            *orden,
        ],
        "checksum_parametros": (
            checksum_parametros(
                parametros_q1
            )
        ),
        "control": control,
        "con_falla": resultados["con_falla"],
        "con_correccion": resultados["con_correccion"],
    }

    nombre = (
        f"EXP-03_{identificador}_"
        f"R{numero:02d}.json"
    )

    destino = RESULTADOS / nombre

    with destino.open(
        "w",
        encoding="utf-8"
    ) as archivo:

        json.dump(
            resultado,
            archivo,
            indent=2,
            ensure_ascii=False,
        )

    print(
        "\nResultado guardado:",
        destino.name
    )

    return resultado


def construir_resumen(
    repeticiones,
    identificador
):

    fases = (
        "control",
        "con_falla",
        "con_correccion",
    )

    metricas = (
        "ejecuciones",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "throughput_ops_s",
        "errores",
        "disponibilidad_pct",
    )

    recursos = (
        "cpu_proceso_pct",
        "cpu_sistema_pct",
        "memoria_proceso_mb",
    )

    resumen_fases = {}

    for fase in fases:

        datos = {}

        for metrica in metricas:

            datos[metrica] = (
                resumen_estadistico([
                    repeticion[fase][metrica]
                    for repeticion in repeticiones
                ])
            )

        datos["recursos"] = {}

        for recurso in recursos:

            datos["recursos"][recurso] = (
                resumen_estadistico([
                    repeticion[fase]["recursos"][
                        recurso
                    ]["promedio"]
                    for repeticion in repeticiones
                ])
            )

        resumen_fases[fase] = datos

    return {
        "tipo": "resumen_experimental",
        "experimento": EXPERIMENTO,
        "identificador_ejecucion": identificador,
        "fecha": datetime.now(
            timezone.utc
        ).isoformat(),
        "repeticiones": len(repeticiones),
        "duracion_fase_s": DURACION_FASE_S,
        "fases": resumen_fases,
        "limitaciones": [
            "La carga CPU es sintética y se "
            "genera mediante hilos Python.",
            "La CPU del proceso no representa "
            "la utilización de PostgreSQL.",
            "La intensidad configurada no "
            "equivale al consumo real observado.",
            "Cada fase utiliza una ventana temporal "
            "de cinco segundos y puede ejecutar "
            "diferente cantidad de consultas.",
            "La corrección reduce la intensidad "
            "de la perturbación, no modifica SQL.",
        ],
    }


def imprimir_resumen(resumen):

    print("\n" + "=" * 65)

    print(
        "SUPPLYGRID - RESUMEN EXP-03"
    )

    print("=" * 65)

    for fase in (
        "control",
        "con_falla",
        "con_correccion",
    ):

        datos = resumen["fases"][fase]

        print(
            f"\n{fase.upper()}"
        )

        print(
            "  p95 mediana:",
            f"{datos['p95_ms']['mediana']:.4f} ms"
        )

        print(
            "  Throughput mediano:",
            f"{datos['throughput_ops_s']['mediana']:.2f} ops/s"
        )

        print(
            "  CPU proceso promedio:",
            f"{datos['recursos']['cpu_proceso_pct']['mediana']:.2f}%"
        )

        print(
            "  CPU sistema promedio:",
            f"{datos['recursos']['cpu_sistema_pct']['mediana']:.2f}%"
        )


def ejecutar():

    RESULTADOS.mkdir(
        parents=True,
        exist_ok=True
    )

    manager = ChaosConfigManager(
        ROOT / "chaos_config.json"
    )

    estado = manager.get_config()

    if estado.kill_switch:
        raise RuntimeError(
            "El kill switch está activo."
        )

    if estado.enabled or any(
        fault.enabled
        for fault in estado.faults.values()
    ):
        raise RuntimeError(
            "El motor debe iniciar deshabilitado."
        )

    engine = ChaosEngine(manager)

    parametros_q1 = preparar_parametros_q1(
        repeticiones=200,
        escenario="frio"
    )

    identificador = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%d_%H%M%S_%f")
    )

    print("=" * 65)
    print("SUPPLYGRID - EXP-03")
    print("=" * 65)

    print(
        "Intensidad falla:",
        f"{INTENSIDAD_FALLA:.0f}%"
    )

    print(
        "Intensidad corrección:",
        f"{INTENSIDAD_CORRECCION:.0f}%"
    )

    print(
        "Duración por fase:",
        f"{DURACION_FASE_S:.1f} s"
    )

    repeticiones = []

    try:

        for numero in range(
            1,
            REPETICIONES_EXPERIMENTALES + 1
        ):

            resultado = ejecutar_repeticion(
                numero,
                parametros_q1,
                manager,
                engine,
                identificador,
            )

            repeticiones.append(
                resultado
            )

    finally:

        engine.stop_all_disturbances()

        manager.reset_defaults()

        print(
            "\nMotor de fallas restaurado "
            "a estado seguro."
        )

    resumen = construir_resumen(
        repeticiones,
        identificador
    )

    imprimir_resumen(resumen)

    destino = RESULTADOS / (
        f"EXP-03_{identificador}_RESUMEN.json"
    )

    with destino.open(
        "w",
        encoding="utf-8"
    ) as archivo:

        json.dump(
            resumen,
            archivo,
            indent=2,
            ensure_ascii=False,
        )

    print(
        "\nResumen guardado en:",
        destino
    )


if __name__ == "__main__":
    ejecutar()