"""
SupplyGrid - Comparador de resultados experimentales E3.

Compara las fases:
    1. Control sin fallas.
    2. Ejecución con falla inyectada.
    3. Ejecución con falla y mitigación.

Calcula:
    - p50, p95 y p99.
    - Degradación porcentual.
    - Mejora porcentual después de la corrección.
    - Cumplimiento de objetivos E3.
    - Throughput.
    - Errores.
    - Estadísticas de caché, cuando corresponda.

Uso:
    python -m experimentos.comparador \
        experimentos/resultados/EXP-01_20261019_184614.json
"""

import csv
import json
import sys
from pathlib import Path


# ============================================================
# RUTAS
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

OBJETIVOS = ROOT / "datos/salida/tabla_objetivos_e3.csv"


# ============================================================
# CARGA DE OBJETIVOS E3
# ============================================================

def cargar_objetivos():
    """
    Carga los objetivos de rendimiento E3.

    La clave de cada objetivo es:
        (consulta, escenario, metrica)
    """

    objetivos = {}

    if not OBJETIVOS.exists():
        print(
            f"ADVERTENCIA: No se encontró el archivo de objetivos: "
            f"{OBJETIVOS}"
        )
        return objetivos

    with OBJETIVOS.open(
        encoding="utf-8-sig",
        newline=""
    ) as archivo:

        lector = csv.DictReader(archivo)

        for fila in lector:

            clave = (
                fila["consulta"].strip(),
                fila["escenario"].strip(),
                fila["metrica"].strip()
            )

            objetivos[clave] = float(
                fila["objetivo_e3_ms"]
            )

    return objetivos


# ============================================================
# FUNCIONES MATEMÁTICAS
# ============================================================

def variacion_porcentual(inicial, final):
    """
    Calcula el cambio porcentual entre dos valores.

    Positivo: incremento.
    Negativo: disminución.
    """

    if (
        inicial is None
        or final is None
        or inicial == 0
    ):
        return None

    return ((final - inicial) / inicial) * 100


def mejora_porcentual(falla, correccion):
    """
    Calcula la reducción porcentual de una métrica
    respecto al escenario con falla.

    Positivo: mejora.
    Negativo: empeoramiento.
    """

    if (
        falla is None
        or correccion is None
        or falla == 0
    ):
        return None

    return ((falla - correccion) / falla) * 100


def formatear_numero(valor, decimales=4):
    """Formatea valores numéricos y maneja datos ausentes."""

    if valor is None:
        return "N/D"

    return f"{valor:.{decimales}f}"


def estado_objetivo(valor, objetivo):
    """Evalúa el cumplimiento de un objetivo E3."""

    if valor is None or objetivo is None:
        return "NO EVALUABLE"

    return "CUMPLE" if valor <= objetivo else "NO CUMPLE"


# ============================================================
# COMPARACIÓN DE LATENCIAS
# ============================================================

def comparar_latencias(
    consulta,
    escenario,
    control,
    falla,
    correccion,
    objetivos
):
    """
    Compara p50, p95 y p99.

    Los objetivos E3 se evalúan únicamente cuando
    existe una referencia para la métrica.
    """

    print("\n" + "=" * 65)
    print("1. COMPARACIÓN DE LATENCIAS")
    print("=" * 65)

    for metrica in ("p50", "p95", "p99"):

        campo = f"{metrica}_ms"

        valor_control = control.get(campo)
        valor_falla = falla.get(campo)

        valor_corregido = (
            correccion.get(campo)
            if correccion is not None
            else None
        )

        objetivo = objetivos.get(
            (consulta, escenario, metrica)
        )

        degradacion = variacion_porcentual(
            valor_control,
            valor_falla
        )

        mejora = mejora_porcentual(
            valor_falla,
            valor_corregido
        )

        print(f"\nMÉTRICA: {metrica.upper()}")
        print("-" * 45)

        print(
            f"  Control sin fallas : "
            f"{formatear_numero(valor_control)} ms"
        )

        print(
            f"  Con falla          : "
            f"{formatear_numero(valor_falla)} ms"
        )

        if correccion is not None:
            print(
                f"  Con corrección     : "
                f"{formatear_numero(valor_corregido)} ms"
            )

        if degradacion is not None:
            print(
                f"  Degradación        : "
                f"{degradacion:+.2f}%"
            )

        if correccion is not None and mejora is not None:

            print(
                f"  Mejora vs falla    : "
                f"{mejora:+.2f}%"
            )

            if mejora > 0:
                print(
                    "  Interpretación     : "
                    "La latencia disminuyó."
                )

            elif mejora < 0:
                print(
                    "  Interpretación     : "
                    "La latencia aumentó."
                )

            else:
                print(
                    "  Interpretación     : "
                    "Sin cambio de latencia."
                )

        if objetivo is not None:

            print(
                f"\n  Objetivo E3        : "
                f"<= {objetivo:.4f} ms"
            )

            print(
                "  Control vs objetivo: "
                + estado_objetivo(valor_control, objetivo)
            )

            print(
                "  Falla vs objetivo  : "
                + estado_objetivo(valor_falla, objetivo)
            )

            if correccion is not None:

                print(
                    "  Corrección vs obj. : "
                    + estado_objetivo(
                        valor_corregido,
                        objetivo
                    )
                )

        else:

            print(
                "\n  Objetivo E3        : "
                "No definido para esta métrica."
            )


# ============================================================
# COMPARACIÓN DE THROUGHPUT
# ============================================================

def comparar_throughput(control, falla, correccion):
    """
    Compara las operaciones exitosas por segundo.

    Un throughput mayor representa más operaciones
    completadas por unidad de tiempo.
    """

    print("\n" + "=" * 65)
    print("2. COMPARACIÓN DE THROUGHPUT")
    print("=" * 65)

    throughput_control = control.get("throughput_ops_s")
    throughput_falla = falla.get("throughput_ops_s")

    print(
        "\n  Control sin fallas : "
        f"{formatear_numero(throughput_control, 2)} ops/s"
    )

    print(
        "  Con falla          : "
        f"{formatear_numero(throughput_falla, 2)} ops/s"
    )

    if correccion is not None:

        throughput_corregido = correccion.get(
            "throughput_ops_s"
        )

        print(
            "  Con corrección     : "
            f"{formatear_numero(throughput_corregido, 2)} ops/s"
        )

        variacion = variacion_porcentual(
            throughput_falla,
            throughput_corregido
        )

        if variacion is not None:

            print(
                f"\n  Variación vs falla : "
                f"{variacion:+.2f}%"
            )


# ============================================================
# COMPARACIÓN DE ERRORES
# ============================================================

def comparar_errores(control, falla, correccion):
    """
    Presenta cantidad de ejecuciones, éxitos y errores.
    """

    print("\n" + "=" * 65)
    print("3. COMPARACIÓN DE ERRORES")
    print("=" * 65)

    fases = [
        ("Control", control),
        ("Con falla", falla)
    ]

    if correccion is not None:
        fases.append(("Con corrección", correccion))

    for nombre, datos in fases:

        ejecuciones = datos.get("ejecuciones", 0)
        exitosas = datos.get("exitosas", 0)
        errores = datos.get("errores", 0)

        tasa_error = (
            (errores / ejecuciones) * 100
            if ejecuciones > 0
            else 0
        )

        print(f"\n  {nombre}:")

        print(f"    Ejecuciones : {ejecuciones}")
        print(f"    Exitosas    : {exitosas}")
        print(f"    Errores     : {errores}")
        print(f"    Tasa error  : {tasa_error:.2f}%")


# ============================================================
# ANÁLISIS DE MITIGACIÓN
# ============================================================

def comparar_mitigacion(falla, correccion):
    """
    Evalúa las estadísticas disponibles de la mitigación.

    Actualmente soporta caché TTL.
    """

    print("\n" + "=" * 65)
    print("4. ANÁLISIS DE MITIGACIÓN")
    print("=" * 65)

    if correccion is None:

        print(
            "\n  No se registró una fase de corrección."
        )

        return

    cache = correccion.get("cache")

    if cache is not None:

        print("\n  Mitigación: caché TTL")

        print(
            f"  Aciertos (hits)    : "
            f"{cache.get('hits', 0)}"
        )

        print(
            f"  Fallos (misses)    : "
            f"{cache.get('misses', 0)}"
        )

        print(
            f"  Tasa de aciertos  : "
            f"{cache.get('hit_rate_pct', 0):.2f}%"
        )

        print(
            f"  Entradas finales  : "
            f"{cache.get('entradas', 0)}"
        )

    else:

        print(
            "\n  No hay estadísticas específicas "
            "de mitigación registradas."
        )

    p95_falla = falla.get("p95_ms")
    p95_corregido = correccion.get("p95_ms")

    p99_falla = falla.get("p99_ms")
    p99_corregido = correccion.get("p99_ms")

    mejora_p95 = mejora_porcentual(
        p95_falla,
        p95_corregido
    )

    mejora_p99 = mejora_porcentual(
        p99_falla,
        p99_corregido
    )

    print("\n  EFECTIVIDAD OBSERVADA")

    print(
        "  Mejora p95: "
        + (
            f"{mejora_p95:+.2f}%"
            if mejora_p95 is not None
            else "N/D"
        )
    )

    print(
        "  Mejora p99: "
        + (
            f"{mejora_p99:+.2f}%"
            if mejora_p99 is not None
            else "N/D"
        )
    )

    if (
        mejora_p95 is not None
        and mejora_p99 is not None
    ):

        if mejora_p95 > 0 and mejora_p99 > 0:

            print(
                "\n  Resultado: se observó una reducción "
                "de ambas latencias."
            )

        elif mejora_p95 <= 0 and mejora_p99 <= 0:

            print(
                "\n  Resultado: no se observó una "
                "mejora en p95 ni p99."
            )

        else:

            print(
                "\n  Resultado: comportamiento mixto "
                "entre p95 y p99."
            )

    print(
        "\n  ADVERTENCIA: Una reducción observada "
        "no demuestra por sí sola una mejora "
        "estadísticamente consistente."
    )


# ============================================================
# COMPARACIÓN GENERAL
# ============================================================

def comparar(resultado):
    """
    Ejecuta la comparación completa del experimento.
    """

    experimento = resultado["experimento"]

    consulta = experimento["consulta"]
    escenario = experimento["escenario"]

    control = resultado["control"]
    falla = resultado["con_falla"]

    # Compatibilidad con experimentos anteriores
    # que todavía no tienen fase de corrección.
    correccion = resultado.get("con_correccion")

    objetivos = cargar_objetivos()

    print("=" * 65)
    print("SUPPLYGRID - COMPARACIÓN EXPERIMENTAL E3")
    print("=" * 65)

    print(f"\nExperimento : {experimento['id']}")
    print(f"Consulta    : {consulta}")
    print(f"Escenario   : {escenario}")

    if "seed" in resultado:
        print(f"Semilla     : {resultado['seed']}")

    comparar_latencias(
        consulta,
        escenario,
        control,
        falla,
        correccion,
        objetivos
    )

    comparar_throughput(
        control,
        falla,
        correccion
    )

    comparar_errores(
        control,
        falla,
        correccion
    )

    comparar_mitigacion(
        falla,
        correccion
    )

    print("\n" + "=" * 65)
    print("5. CONCLUSIONES Y LIMITACIONES")
    print("=" * 65)

    print(
        "\n- La degradación se calcula respecto "
        "al control sin fallas."
    )

    print(
        "- La mejora se calcula respecto "
        "a la fase con falla."
    )

    print(
        "- Los objetivos E3 se comparan con "
        "las referencias históricas del proyecto."
    )

    print(
        "- Una mitigación con caché puede reducir "
        "las consultas reales a PostgreSQL."
    )

    print(
        "- El cumplimiento definitivo requiere "
        "repeticiones y condiciones comparables."
    )

    print("\n" + "=" * 65)
    print("FIN DEL ANÁLISIS")
    print("=" * 65)


# ============================================================
# PUNTO DE ENTRADA
# ============================================================

def main():

    if len(sys.argv) != 2:

        print(
            "Uso:\n"
            "python -m experimentos.comparador "
            "<archivo_resultados.json>"
        )

        sys.exit(1)

    ruta = Path(sys.argv[1])

    if not ruta.exists():

        print(
            f"ERROR: No existe el archivo: {ruta}"
        )

        sys.exit(1)

    try:

        with ruta.open(encoding="utf-8") as archivo:
            resultado = json.load(archivo)

        comparar(resultado)

    except (KeyError, ValueError, TypeError) as error:

        print(
            f"ERROR: El archivo contiene datos "
            f"inválidos o incompletos: {error}"
        )

        sys.exit(1)


if __name__ == "__main__":
    main()