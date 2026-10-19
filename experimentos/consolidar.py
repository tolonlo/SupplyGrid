"""
SupplyGrid - Consolidación de resultados E3.

Lee los resúmenes definitivos de EXP-01 a EXP-04
y genera tablas comparativas en CSV y JSON.

No modifica los resultados experimentales originales.
"""

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

RESULTADOS = ROOT / "experimentos" / "resultados"

SALIDA = ROOT / "experimentos" / "consolidado"

EXPERIMENTOS = {
    "EXP-01": "EXP-01_20261019_185521_631240_RESUMEN.json",
    "EXP-02": "EXP-02_20261019_190110_193274_RESUMEN.json",
    "EXP-03": "EXP-03_20261019_191607_348795_RESUMEN.json",
    "EXP-04": "EXP-04_20261019_191911_939186_RESUMEN.json",
}

FASES = (
    "control",
    "con_falla",
    "con_correccion",
)

METRICAS = (
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "throughput_ops_s",
    "disponibilidad_pct",
    "errores",
)


def cargar_resumen(experimento, archivo):
    """Carga un resumen experimental."""

    ruta = RESULTADOS / archivo

    if not ruta.exists():
        raise FileNotFoundError(
            f"No se encontró el resumen de {experimento}: {ruta}"
        )

    with ruta.open("r", encoding="utf-8") as f:
        return json.load(f)


def obtener_mediana(resumen, fase, metrica):
    """
    Extrae la mediana de una métrica.

    Devuelve None si no está disponible.
    """

    datos_fase = resumen.get("fases", {}).get(fase, {})

    datos_metrica = datos_fase.get(metrica)

    if datos_metrica is None:
        return None

    if isinstance(datos_metrica, dict):
        return datos_metrica.get("mediana")

    return None


def cambio_porcentual(original, nuevo):
    """Calcula el cambio porcentual respecto al original."""

    if original is None or nuevo is None:
        return None

    if original == 0:
        return None

    return ((nuevo - original) / original) * 100


def reduccion_porcentual(original, nuevo):
    """Calcula cuánto se redujo una métrica."""

    if original is None or nuevo is None:
        return None

    if original == 0:
        return None

    return ((original - nuevo) / original) * 100


def guardar_csv(ruta, filas, columnas):
    """Guarda un archivo CSV."""

    with ruta.open(
        "w",
        newline="",
        encoding="utf-8-sig"
    ) as archivo:

        escritor = csv.DictWriter(
            archivo,
            fieldnames=columnas
        )

        escritor.writeheader()

        escritor.writerows(filas)


def consolidar():

    SALIDA.mkdir(
        parents=True,
        exist_ok=True
    )

    filas_metricas = []

    filas_mitigaciones = []

    datos_completos = {}

    for experimento, archivo in EXPERIMENTOS.items():

        resumen = cargar_resumen(
            experimento,
            archivo
        )

        datos_completos[experimento] = resumen

        for fase in FASES:

            fila = {
                "experimento": experimento,
                "fase": fase,
            }

            for metrica in METRICAS:

                fila[metrica] = obtener_mediana(
                    resumen,
                    fase,
                    metrica
                )

            if experimento == "EXP-03":

                recursos = (
                    resumen.get("fases", {})
                    .get(fase, {})
                    .get("recursos", {})
                )

                cpu = recursos.get(
                    "cpu_proceso_pct",
                    {}
                )

                fila["cpu_proceso_pct"] = (
                    cpu.get("mediana")
                    if isinstance(cpu, dict)
                    else None
                )

            else:

                fila["cpu_proceso_pct"] = None

            filas_metricas.append(fila)

        p95_falla = obtener_mediana(
            resumen,
            "con_falla",
            "p95_ms"
        )

        p95_correccion = obtener_mediana(
            resumen,
            "con_correccion",
            "p95_ms"
        )

        throughput_falla = obtener_mediana(
            resumen,
            "con_falla",
            "throughput_ops_s"
        )

        throughput_correccion = obtener_mediana(
            resumen,
            "con_correccion",
            "throughput_ops_s"
        )

        disponibilidad_falla = obtener_mediana(
            resumen,
            "con_falla",
            "disponibilidad_pct"
        )

        disponibilidad_correccion = obtener_mediana(
            resumen,
            "con_correccion",
            "disponibilidad_pct"
        )

        errores_falla = obtener_mediana(
            resumen,
            "con_falla",
            "errores"
        )

        errores_correccion = obtener_mediana(
            resumen,
            "con_correccion",
            "errores"
        )

        fila_mitigacion = {
            "experimento": experimento,
            "reduccion_p95_pct": reduccion_porcentual(
                p95_falla,
                p95_correccion
            ),
            "cambio_throughput_pct": cambio_porcentual(
                throughput_falla,
                throughput_correccion
            ),
            "recuperacion_disponibilidad_puntos": (
                disponibilidad_correccion
                - disponibilidad_falla
                if disponibilidad_falla is not None
                and disponibilidad_correccion is not None
                else None
            ),
            "reduccion_errores_pct": reduccion_porcentual(
                errores_falla,
                errores_correccion
            ),
        }

        filas_mitigaciones.append(
            fila_mitigacion
        )

    columnas_metricas = [
        "experimento",
        "fase",
        *METRICAS,
        "cpu_proceso_pct",
    ]

    columnas_mitigaciones = [
        "experimento",
        "reduccion_p95_pct",
        "cambio_throughput_pct",
        "recuperacion_disponibilidad_puntos",
        "reduccion_errores_pct",
    ]

    guardar_csv(
        SALIDA / "consolidado_experimentos.csv",
        filas_metricas,
        columnas_metricas
    )

    guardar_csv(
        SALIDA / "comparacion_mitigaciones.csv",
        filas_mitigaciones,
        columnas_mitigaciones
    )

    with (
        SALIDA / "consolidado_experimentos.json"
    ).open("w", encoding="utf-8") as archivo:

        json.dump(
            {
                "experimentos": datos_completos,
                "metricas": filas_metricas,
                "mitigaciones": filas_mitigaciones,
            },
            archivo,
            indent=2,
            ensure_ascii=False
        )

    print("=" * 65)
    print("SUPPLYGRID - CONSOLIDACIÓN EXPERIMENTAL")
    print("=" * 65)

    print("\nExperimentos procesados:", len(EXPERIMENTOS))

    print("Fases consolidadas:", len(filas_metricas))

    print("\nCOMPARACIÓN DE MITIGACIONES")

    for fila in filas_mitigaciones:

        print(f"\n{fila['experimento']}")

        for clave, valor in fila.items():

            if clave == "experimento":
                continue

            if valor is None:
                print(f"  {clave}: N/D")

            else:
                print(
                    f"  {clave}: {valor:.2f}"
                )

    print("\nArchivos generados:")

    for archivo in sorted(SALIDA.iterdir()):
        print(" ", archivo)


if __name__ == "__main__":
    consolidar()