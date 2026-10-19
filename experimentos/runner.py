"""
SupplyGrid - Ejecutor experimental E3.

EXP-01:
    Familia A - Latencia inducida.
    Consulta Q1 - Contratos vigentes.
    Escenario frío.

Protocolo:
    - 200 solicitudes por fase.
    - Semilla 42.
    - 5 repeticiones experimentales.
    - Control sin fallas.
    - Falla A1 activa.
    - Falla A1 + mitigación con caché TTL.

Genera:
    - Un JSON por repetición.
    - Un JSON agregado con estadísticas.
    - Un manifiesto de la ejecución.

IMPORTANTE:
    Ejecutar únicamente en un laboratorio local dedicado.

    El programa restablece la configuración de fallas
    mediante reset_defaults() al finalizar. Esto puede
    sobrescribir configuraciones personalizadas.
"""

import hashlib
import json
import os
import random
import statistics
import time

from datetime import datetime, timezone
from pathlib import Path

import psycopg2

from chaos.config import ChaosConfigManager
from chaos.interceptor import connect_with_chaos
from chaos import observabilidad as obs

from datos.medir_consultas import (
    obtener_proveedores,
    construir_parametros,
)

from experimentos.mitigaciones import CacheTTL


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

CONFIG_PATH = ROOT / "experimentos/config_experimentos.json"

CHAOS_CONFIG_PATH = ROOT / "chaos_config.json"

RESULTADOS = ROOT / "experimentos/resultados"

RESULTADOS.mkdir(parents=True, exist_ok=True)

REPETICIONES_EXPERIMENTALES = 5

SEED = 42

TTL_CACHE_SEGUNDOS = 60


SQL_Q1 = """
SELECT id, proveedor_id, fecha_inicio, fecha_fin, estado
FROM proveedores.contratos
WHERE proveedor_id = %s
  AND fecha_inicio <= %s
  AND fecha_fin >= %s
  AND estado = 'VIGENTE';
"""


# ============================================================
# CONEXIÓN A POSTGRESQL
# ============================================================

def conectar(con_chaos=False):

    configuracion = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "postgres"),
        "dbname": os.getenv("PGDATABASE", "supplygrid_db"),
    }

    if con_chaos:
        conexion = connect_with_chaos(**configuracion)
    else:
        conexion = psycopg2.connect(**configuracion)

    conexion.autocommit = True

    return conexion


# ============================================================
# ESTADÍSTICAS
# ============================================================

def percentil(valores, porcentaje):

    if not valores:
        return None

    ordenados = sorted(valores)

    posicion = (len(ordenados) - 1) * porcentaje

    inferior = int(posicion)

    superior = min(
        inferior + 1,
        len(ordenados) - 1
    )

    fraccion = posicion - inferior

    return (
        ordenados[inferior]
        + (
            ordenados[superior]
            - ordenados[inferior]
        ) * fraccion
    )


def resumen_estadistico(valores):
    """
    Resume una métrica entre repeticiones.

    La desviación estándar es muestral.
    """

    validos = [
        valor
        for valor in valores
        if valor is not None
    ]

    if not validos:
        return {
            "n": 0,
            "mediana": None,
            "media": None,
            "minimo": None,
            "maximo": None,
            "desviacion_estandar": None,
        }

    return {
        "n": len(validos),
        "mediana": statistics.median(validos),
        "media": statistics.mean(validos),
        "minimo": min(validos),
        "maximo": max(validos),
        "desviacion_estandar": (
            statistics.stdev(validos)
            if len(validos) >= 2
            else None
        ),
    }


def mejora_porcentual(valor_falla, valor_corregido):

    if (
        valor_falla is None
        or valor_corregido is None
        or valor_falla == 0
    ):
        return None

    return (
        (valor_falla - valor_corregido)
        / valor_falla
    ) * 100


# ============================================================
# PREPARACIÓN DE PARÁMETROS
# ============================================================

def preparar_parametros_q1(
    repeticiones=200,
    escenario="frio"
):
    """
    Reutiliza las funciones de selección del protocolo E2.

    La preparación queda fuera del intervalo medido.
    """

    if repeticiones != 200:
        raise ValueError(
            "El protocolo E2 requiere 200 repeticiones."
        )

    if escenario not in ("frio", "caliente"):
        raise ValueError(
            f"Escenario no válido: {escenario}"
        )

    rng = random.Random(SEED)

    conexion = conectar()

    try:
        cursor = conexion.cursor()

        frios, calientes = obtener_proveedores(
            cursor
        )

        proveedores = (
            frios
            if escenario == "frio"
            else calientes
        )

        base = construir_parametros(
            cursor,
            proveedores,
            rng
        )

        return [
            (
                dato["proveedor"],
                "2026-09-18 10:00:00",
                "2026-09-18 10:00:00",
            )
            for dato in base
        ]

    finally:
        conexion.close()


def checksum_parametros(parametros):
    """
    Genera una huella de la secuencia de parámetros.

    Permite verificar que todas las fases utilizan
    exactamente la misma carga lógica.
    """

    contenido = json.dumps(
        parametros,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        contenido.encode("utf-8")
    ).hexdigest()


# ============================================================
# MEDICIÓN
# ============================================================

def medir(
    parametros_q1,
    usar_cache=False,
    ttl_segundos=TTL_CACHE_SEGUNDOS
):
    """
    Mide 200 solicitudes lógicas.

    Sin caché:
        Cada solicitud ejecuta SQL.

    Con caché:
        Solo los misses ejecutan SQL.

    La caché empieza vacía en cada llamada.
    """

    cache = None

    if usar_cache:
        cache = CacheTTL(
            ttl_segundos=ttl_segundos,
            capacidad=1000,
        )

    tiempos = []
    errores = []

    consultas_sql = 0

    conexion = conectar(con_chaos=True)

    inicio_global = time.perf_counter()

    try:
        cursor = conexion.cursor()

        for numero, parametros in enumerate(
            parametros_q1,
            start=1
        ):

            inicio = time.perf_counter_ns()

            try:

                if usar_cache:

                    clave = ("Q1", parametros)

                    resultado = cache.obtener(
                        clave
                    )

                    if resultado is None:

                        consultas_sql += 1

                        cursor.execute(
                            SQL_Q1,
                            parametros
                        )

                        resultado = cursor.fetchall()

                        cache.guardar(
                            clave,
                            resultado
                        )

                else:

                    consultas_sql += 1

                    cursor.execute(
                        SQL_Q1,
                        parametros
                    )

                    cursor.fetchall()

                fin = time.perf_counter_ns()

                tiempos.append(
                    (fin - inicio) / 1_000_000
                )

            except Exception as error:

                errores.append({
                    "ejecucion": numero,
                    "tipo": type(error).__name__,
                    "mensaje": str(error),
                })

    finally:

        duracion = (
            time.perf_counter()
            - inicio_global
        )

        conexion.close()

    resultado = {
        "ejecuciones": len(parametros_q1),
        "exitosas": len(tiempos),
        "errores": len(errores),
        "consultas_sql_ejecutadas": consultas_sql,
        "p50_ms": percentil(tiempos, 0.50),
        "p95_ms": percentil(tiempos, 0.95),
        "p99_ms": percentil(tiempos, 0.99),
        "duracion_total_s": duracion,
        "throughput_ops_s": (
            len(tiempos) / duracion
            if duracion > 0
            else 0
        ),
        "detalle_errores": errores,
        "tiempos_crudos_ms": tiempos,
    }

    if cache is not None:
        resultado["cache"] = cache.estadisticas()

    return resultado


# ============================================================
# CONFIGURACIÓN DEL MOTOR
# ============================================================

def verificar_estado_inicial(manager):
    """
    Impide iniciar con fallas previamente activadas.
    """

    estado = manager.get_config()

    if estado.kill_switch:
        raise RuntimeError(
            "El kill switch está activo."
        )

    activas = [
        nombre
        for nombre, falla in estado.faults.items()
        if falla.enabled
    ]

    if activas:
        raise RuntimeError(
            "Hay fallas activas: "
            + ", ".join(activas)
        )

    if estado.enabled:
        raise RuntimeError(
            "El motor global está habilitado. "
            "Desactívalo antes de iniciar."
        )


def activar_falla(manager, experimento):
    obs.falla(experimento["fault_id"], True)

    manager.set_fault_state(
        experimento["fault_id"],
        True,
        parameters=experimento["parametros"],
        traffic_pct=1.0,
    )


def desactivar_falla(manager, experimento):
    obs.falla(experimento["fault_id"], False)

    manager.set_fault_state(
        experimento["fault_id"],
        False,
    )


# ============================================================
# EJECUCIÓN DE UNA REPETICIÓN
# ============================================================

def ejecutar_repeticion(
    numero,
    experimento,
    parametros_q1,
    manager,
    identificador_ejecucion
):
    """
    Ejecuta control, falla y corrección.

    Se alterna el orden de las dos fases perturbadas:

    Repeticiones impares:
        Control -> Falla -> Corrección

    Repeticiones pares:
        Control -> Corrección -> Falla
    """

    print("\n" + "=" * 65)

    print(
        f"REPETICIÓN {numero}/"
        f"{REPETICIONES_EXPERIMENTALES}"
    )

    print("=" * 65)

    print("\nFASE CONTROL")
    obs.abrir_fase(experimento["id"], "control")

    control = medir(
        parametros_q1,
        usar_cache=False,
    )

    print(
        f"  p95: {control['p95_ms']:.4f} ms"
    )

    print(
        f"  p99: {control['p99_ms']:.4f} ms"
    )

    obs.cerrar_fase()
    if numero % 2 == 1:

        orden = [
            "con_falla",
            "con_correccion",
        ]

    else:

        orden = [
            "con_correccion",
            "con_falla",
        ]

    resultados_fases = {}

    try:

        activar_falla(
            manager,
            experimento
        )

        for fase in orden:
            obs.abrir_fase(experimento["id"], fase)

            if fase == "con_falla":

                print("\nFASE FALLA A1")

                resultado = medir(
                    parametros_q1,
                    usar_cache=False,
                )

            else:

                print(
                    "\nFASE A1 + CACHÉ TTL"
                )

                resultado = medir(
                    parametros_q1,
                    usar_cache=True,
                    ttl_segundos=TTL_CACHE_SEGUNDOS,
                )

            resultados_fases[fase] = resultado
            obs.cerrar_fase()

            print(
                f"  p95: {resultado['p95_ms']:.4f} ms"
            )

            print(
                f"  p99: {resultado['p99_ms']:.4f} ms"
            )

            if fase == "con_correccion":

                print(
                    "  Aciertos caché: "
                    f"{resultado['cache']['hit_rate_pct']:.2f}%"
                )

    finally:

        desactivar_falla(
            manager,
            experimento
        )

    con_falla = resultados_fases["con_falla"]

    con_correccion = resultados_fases[
        "con_correccion"
    ]

    resultado_repeticion = {
        "experimento": experimento,
        "fecha": datetime.now(
            timezone.utc
        ).isoformat(),
        "identificador_ejecucion": identificador_ejecucion,
        "numero_repeticion": numero,
        "orden_fases": [
            "control",
            *orden,
        ],
        "escenario": experimento["escenario"],
        "seed": SEED,
        "checksum_parametros": checksum_parametros(
            parametros_q1
        ),
        "control": control,
        "con_falla": con_falla,
        "con_correccion": con_correccion,
        "incremento_p95_pct": (
            (
                con_falla["p95_ms"]
                - control["p95_ms"]
            )
            / control["p95_ms"] * 100
            if control["p95_ms"]
            else None
        ),
        "mejora_p95_pct": mejora_porcentual(
            con_falla["p95_ms"],
            con_correccion["p95_ms"],
        ),
        "mejora_p99_pct": mejora_porcentual(
            con_falla["p99_ms"],
            con_correccion["p99_ms"],
        ),
    }

    nombre = (
        f"{experimento['id']}_"
        f"{identificador_ejecucion}_"
        f"R{numero:02d}.json"
    )

    destino = RESULTADOS / nombre

    with destino.open(
        "w",
        encoding="utf-8"
    ) as archivo:

        json.dump(
            resultado_repeticion,
            archivo,
            indent=2,
            ensure_ascii=False,
        )

    print(
        f"\nRepetición guardada: {destino.name}"
    )

    return resultado_repeticion


# ============================================================
# RESUMEN AGREGADO
# ============================================================

def construir_resumen(
    experimento,
    repeticiones,
    identificador_ejecucion
):
    """
    Calcula estadísticas entre las cinco repeticiones.

    No mezcla las 200 mediciones individuales para
    calcular un único percentil agregado.
    """

    fases = [
        "control",
        "con_falla",
        "con_correccion",
    ]

    metricas = [
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "throughput_ops_s",
        "errores",
        "consultas_sql_ejecutadas",
    ]

    resumen_fases = {}

    for fase in fases:

        resumen_fases[fase] = {}

        for metrica in metricas:

            valores = [
                repeticion[fase].get(metrica)
                for repeticion in repeticiones
            ]

            resumen_fases[fase][
                metrica
            ] = resumen_estadistico(valores)

    mejoras = {
        "p95_pct": resumen_estadistico([
            repeticion["mejora_p95_pct"]
            for repeticion in repeticiones
        ]),
        "p99_pct": resumen_estadistico([
            repeticion["mejora_p99_pct"]
            for repeticion in repeticiones
        ]),
        "hit_rate_pct": resumen_estadistico([
            repeticion["con_correccion"]["cache"][
                "hit_rate_pct"
            ]
            for repeticion in repeticiones
        ]),
    }

    return {
        "tipo": "resumen_experimental",
        "experimento": experimento,
        "identificador_ejecucion": identificador_ejecucion,
        "fecha": datetime.now(
            timezone.utc
        ).isoformat(),
        "repeticiones": len(repeticiones),
        "solicitudes_por_fase": experimento[
            "repeticiones"
        ],
        "seed": SEED,
        "checksum_parametros": (
            repeticiones[0]["checksum_parametros"]
            if repeticiones
            else None
        ),
        "fases": resumen_fases,
        "mejoras": mejoras,
        "limitaciones": [
            "Las repeticiones utilizan los mismos "
            "parámetros y la misma semilla.",
            "La caché reduce las consultas SQL "
            "ejecutadas y modifica la carga real "
            "sobre PostgreSQL.",
            "La caché es local, comienza vacía "
            "y no implementa invalidación "
            "por modificaciones de contratos.",
            "Las fases se ejecutan secuencialmente "
            "y pueden existir efectos de orden.",
            "Los resultados describen este entorno "
            "de laboratorio y no demuestran "
            "cumplimiento general del sistema.",
        ],
    }


def imprimir_resumen(resumen):

    print("\n" + "=" * 65)

    print(
        "SUPPLYGRID - RESUMEN DE "
        "REPETICIONES EXP-01"
    )

    print("=" * 65)

    for fase in (
        "control",
        "con_falla",
        "con_correccion",
    ):

        datos = resumen["fases"][fase]

        p95 = datos["p95_ms"]["mediana"]
        p99 = datos["p99_ms"]["mediana"]

        throughput = datos[
            "throughput_ops_s"
        ]["mediana"]

        print(f"\n{fase.upper()}")

        print(f"  Mediana p95: {p95:.4f} ms")
        print(f"  Mediana p99: {p99:.4f} ms")

        print(
            f"  Mediana throughput: "
            f"{throughput:.2f} ops/s"
        )

    print("\nMEJORAS ENTRE REPETICIONES")

    for metrica in ("p95_pct", "p99_pct"):

        datos = resumen["mejoras"][metrica]

        print(
            f"  {metrica}: "
            f"mediana={datos['mediana']:+.2f}% | "
            f"mín={datos['minimo']:+.2f}% | "
            f"máx={datos['maximo']:+.2f}%"
        )

    print(
        "\n  Mediana de aciertos de caché: "
        f"{resumen['mejoras']['hit_rate_pct']['mediana']:.2f}%"
    )


# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================

def ejecutar():

    with CONFIG_PATH.open(
        encoding="utf-8"
    ) as archivo:

        configuracion = json.load(archivo)

    experimento = configuracion["experimentos"][0]

    if experimento["id"] != "EXP-01":
        raise ValueError(
            "Esta versión del ejecutor está "
            "implementada únicamente para EXP-01."
        )

    if experimento["consulta"] != "Q1":
        raise ValueError(
            "Esta versión solo ejecuta Q1."
        )

    manager = ChaosConfigManager(
        CHAOS_CONFIG_PATH
    )

    verificar_estado_inicial(manager)
    obs.iniciar()

    identificador_ejecucion = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%d_%H%M%S_%f")
    )

    print("=" * 65)

    print(
        "SUPPLYGRID - EXPERIMENTO EXP-01"
    )

    print("=" * 65)

    print(
        f"Repeticiones: "
        f"{REPETICIONES_EXPERIMENTALES}"
    )

    print(
        f"Consultas por fase: "
        f"{experimento['repeticiones']}"
    )

    print(
        f"Escenario: "
        f"{experimento['escenario']}"
    )

    parametros_q1 = preparar_parametros_q1(
        repeticiones=experimento[
            "repeticiones"
        ],
        escenario=experimento[
            "escenario"
        ],
    )

    checksum = checksum_parametros(
        parametros_q1
    )

    print(
        f"\nChecksum de parámetros: {checksum}"
    )

    repeticiones = []

    try:

        for numero in range(
            1,
            REPETICIONES_EXPERIMENTALES + 1
        ):

            resultado = ejecutar_repeticion(
                numero=numero,
                experimento=experimento,
                parametros_q1=parametros_q1,
                manager=manager,
                identificador_ejecucion=(
                    identificador_ejecucion
                ),
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

    resumen = construir_resumen(
        experimento=experimento,
        repeticiones=repeticiones,
        identificador_ejecucion=(
            identificador_ejecucion
        ),
    )

    nombre_resumen = (
        f"{experimento['id']}_"
        f"{identificador_ejecucion}_"
        "RESUMEN.json"
    )

    destino = RESULTADOS / nombre_resumen

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

    imprimir_resumen(resumen)

    print(
        "\nResumen agregado guardado en:"
    )

    print(destino)

    print(
        "\nExperimento finalizado."
    )


if __name__ == "__main__":
    ejecutar()