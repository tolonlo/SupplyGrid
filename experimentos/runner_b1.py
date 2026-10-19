"""
SupplyGrid - EXP-02.

Familia B:
    B1 - Agotamiento simulado del pool.

Objetivo:
    Evaluar disponibilidad de Q1 ante rechazos
    probabilísticos y recuperación mediante
    reintentos acotados con backoff exponencial.

IMPORTANTE:
    No simula el agotamiento físico del pool.
    Evalúa los rechazos producidos por ChaosEngine.
"""

import json
import random
import statistics
import time

from datetime import datetime, timezone

from chaos.config import ChaosConfigManager
from chaos.engine import ChaosDatabaseError

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
    "id": "EXP-02",
    "familia": "B",
    "fault_id": "B1_pool_agotado",
    "consulta": "Q1",
    "escenario": "frio",
    "repeticiones": 200,
    "seed": 42,
    "traffic_pct": 0.30,
    "hipotesis": (
        "Los rechazos simulados B1 reducirán la "
        "disponibilidad. Los reintentos acotados "
        "recuperarán parte de las solicitudes."
    ),
}

REPETICIONES_EXPERIMENTALES = 5

MAX_INTENTOS = 3

BACKOFF_INICIAL_S = 0.005


def medir_b1(conexion, parametros_q1, reintentos=False):
    """
    Ejecuta solicitudes sobre una conexión existente.

    La conexión se abre antes de activar B1.

    Los reintentos se aplican únicamente a
    ChaosDatabaseError.
    """

    cursor = conexion.cursor()

    tiempos_exitosos = []
    tiempos_todas = []

    errores = []

    intentos_totales = 0
    solicitudes_recuperadas = 0

    inicio_global = time.perf_counter()

    for numero, parametros in enumerate(
        parametros_q1,
        start=1
    ):

        inicio = time.perf_counter_ns()

        exitoso = False
        ultimo_error = None

        max_intentos = (
            MAX_INTENTOS
            if reintentos
            else 1
        )

        for intento in range(
            1,
            max_intentos + 1
        ):

            intentos_totales += 1

            try:

                cursor.execute(
                    SQL_Q1,
                    parametros
                )

                cursor.fetchall()

                exitoso = True

                if intento > 1:
                    solicitudes_recuperadas += 1

                break

            except ChaosDatabaseError as error:

                ultimo_error = error

                if intento < max_intentos:

                    espera = (
                        BACKOFF_INICIAL_S
                        * (2 ** (intento - 1))
                    )

                    time.sleep(espera)

            except Exception as error:

                ultimo_error = error
                break

        fin = time.perf_counter_ns()

        duracion_ms = (
            fin - inicio
        ) / 1_000_000

        tiempos_todas.append(
            duracion_ms
        )

        if exitoso:

            tiempos_exitosos.append(
                duracion_ms
            )

        else:

            errores.append({
                "ejecucion": numero,
                "tipo": (
                    type(ultimo_error).__name__
                    if ultimo_error
                    else "ErrorDesconocido"
                ),
                "mensaje": (
                    str(ultimo_error)
                    if ultimo_error
                    else "Sin detalle"
                ),
            })

    duracion_total = (
        time.perf_counter()
        - inicio_global
    )

    exitosas = len(tiempos_exitosos)

    total = len(parametros_q1)

    return {
        "ejecuciones": total,
        "exitosas": exitosas,
        "errores": len(errores),
        "disponibilidad_pct": (
            exitosas / total * 100
            if total
            else 0
        ),
        "intentos_totales": intentos_totales,
        "solicitudes_recuperadas": solicitudes_recuperadas,
        "p50_ms": percentil(
            tiempos_exitosos,
            0.50
        ),
        "p95_ms": percentil(
            tiempos_exitosos,
            0.95
        ),
        "p99_ms": percentil(
            tiempos_exitosos,
            0.99
        ),
        "p95_todas_ms": percentil(
            tiempos_todas,
            0.95
        ),
        "p99_todas_ms": percentil(
            tiempos_todas,
            0.99
        ),
        "duracion_total_s": duracion_total,
        "throughput_ops_s": (
            exitosas / duracion_total
            if duracion_total > 0
            else 0
        ),
        "detalle_errores": errores,
        "tiempos_crudos_ms": tiempos_exitosos,
        "tiempos_todas_ms": tiempos_todas,
    }


def ejecutar_repeticion(
    numero,
    manager,
    parametros_q1,
    identificador
):
    """
    Ejecuta tres fases:

        Control.
        B1 sin corrección.
        B1 con reintentos.

    Alterna el orden de las fases perturbadas.
    """

    print(
        "\n" + "=" * 65
    )

    print(
        f"EXP-02 | REPETICIÓN "
        f"{numero}/{REPETICIONES_EXPERIMENTALES}"
    )

    print("=" * 65)

    conexion = conectar(
        con_chaos=True
    )

    try:

        print("\nFASE CONTROL")

        control = medir_b1(
            conexion,
            parametros_q1,
            reintentos=False
        )

        print(
            "  Disponibilidad:",
            f"{control['disponibilidad_pct']:.2f}%"
        )

        print(
            "  Errores:",
            control["errores"]
        )

        orden = (
            ["con_falla", "con_correccion"]
            if numero % 2
            else ["con_correccion", "con_falla"]
        )

        resultados = {}

        manager.set_fault_state(
            EXPERIMENTO["fault_id"],
            True,
            traffic_pct=EXPERIMENTO["traffic_pct"],
        )

        try:

            for fase in orden:

                usar_reintentos = (
                    fase == "con_correccion"
                )

                print(
                    "\nFASE:",
                    fase
                )

                resultado = medir_b1(
                    conexion,
                    parametros_q1,
                    reintentos=usar_reintentos
                )

                resultados[fase] = resultado

                print(
                    "  Disponibilidad:",
                    f"{resultado['disponibilidad_pct']:.2f}%"
                )

                print(
                    "  Errores:",
                    resultado["errores"]
                )

                print(
                    "  Intentos:",
                    resultado["intentos_totales"]
                )

        finally:

            manager.set_fault_state(
                EXPERIMENTO["fault_id"],
                False
            )

    finally:

        conexion.close()

    resultado = {
        "experimento": EXPERIMENTO,
        "fecha": datetime.now(
            timezone.utc
        ).isoformat(),
        "numero_repeticion": numero,
        "identificador_ejecucion": identificador,
        "orden_fases": [
            "control",
            *orden
        ],
        "seed": 42,
        "checksum_parametros": checksum_parametros(
            parametros_q1
        ),
        "control": control,
        "con_falla": resultados["con_falla"],
        "con_correccion": resultados["con_correccion"],
    }

    nombre = (
        f"EXP-02_{identificador}_"
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
            ensure_ascii=False
        )

    print(
        "\nResultado guardado:",
        destino.name
    )

    return resultado


def construir_resumen_b1(repeticiones, identificador):

    fases = (
        "control",
        "con_falla",
        "con_correccion",
    )

    metricas = (
        "disponibilidad_pct",
        "errores",
        "p95_ms",
        "p99_ms",
        "p95_todas_ms",
        "p99_todas_ms",
        "throughput_ops_s",
        "intentos_totales",
        "solicitudes_recuperadas",
    )

    resumen = {}

    for fase in fases:

        resumen[fase] = {}

        for metrica in metricas:

            resumen[fase][metrica] = (
                resumen_estadistico([
                    repeticion[fase][metrica]
                    for repeticion in repeticiones
                ])
            )

    return {
        "tipo": "resumen_experimental",
        "experimento": EXPERIMENTO,
        "identificador_ejecucion": identificador,
        "fecha": datetime.now(
            timezone.utc
        ).isoformat(),
        "repeticiones": len(repeticiones),
        "fases": resumen,
        "limitaciones": [
            "B1 es una simulación de rechazo "
            "y no un agotamiento físico del pool.",
            "La conexión se establece antes "
            "de activar la falla.",
            "El muestreo de tráfico utiliza "
            "aleatoriedad no determinista "
            "en el interceptor actual.",
            "Los percentiles principales "
            "consideran solicitudes exitosas.",
            "Los tiempos de solicitudes fallidas "
            "se registran por separado.",
        ],
    }


def imprimir_resumen_b1(resumen):

    print(
        "\n" + "=" * 65
    )

    print(
        "SUPPLYGRID - RESUMEN EXP-02"
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
            "  Disponibilidad mediana:",
            f"{datos['disponibilidad_pct']['mediana']:.2f}%"
        )

        print(
            "  Errores mediana:",
            f"{datos['errores']['mediana']:.1f}"
        )

        print(
            "  p95 exitosas:",
            f"{datos['p95_ms']['mediana']:.4f} ms"
        )

        print(
            "  Throughput:",
            f"{datos['throughput_ops_s']['mediana']:.2f} ops/s"
        )

    print(
        "\nResultados agregados listos."
    )


def ejecutar():

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

    parametros_q1 = preparar_parametros_q1(
        repeticiones=200,
        escenario="frio"
    )

    identificador = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%d_%H%M%S_%f")
    )

    print("=" * 65)
    print("SUPPLYGRID - EXP-02")
    print("=" * 65)

    print(
        "Falla:",
        EXPERIMENTO["fault_id"]
    )

    print(
        "Tráfico afectado:",
        f"{EXPERIMENTO['traffic_pct'] * 100:.0f}%"
    )

    print(
        "Repeticiones:",
        REPETICIONES_EXPERIMENTALES
    )

    repeticiones = []

    try:

        for numero in range(
            1,
            REPETICIONES_EXPERIMENTALES + 1
        ):

            resultado = ejecutar_repeticion(
                numero,
                manager,
                parametros_q1,
                identificador
            )

            repeticiones.append(
                resultado
            )

    finally:

        manager.reset_defaults()

        print(
            "\nMotor de fallas restaurado "
            "a estado seguro."
        )

    resumen = construir_resumen_b1(
        repeticiones,
        identificador
    )

    imprimir_resumen_b1(
        resumen
    )

    nombre = (
        f"EXP-02_{identificador}_RESUMEN.json"
    )

    destino = RESULTADOS / nombre

    with destino.open(
        "w",
        encoding="utf-8"
    ) as archivo:

        json.dump(
            resumen,
            archivo,
            indent=2,
            ensure_ascii=False
        )

    print(
        "\nResumen guardado en:",
        destino
    )


if __name__ == "__main__":
    ejecutar()