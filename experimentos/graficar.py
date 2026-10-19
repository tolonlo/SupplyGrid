"""
SupplyGrid - Gráficas de resultados experimentales E3.

Genera una gráfica por experimento utilizando los
resultados consolidados en formato CSV.

No modifica los resultados originales.
"""

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]

CONSOLIDADO = ROOT / "experimentos" / "consolidado"

GRAFICAS = ROOT / "experimentos" / "graficas"

ARCHIVO_CSV = CONSOLIDADO / "consolidado_experimentos.csv"

FASES = (
    "control",
    "con_falla",
    "con_correccion",
)

ETIQUETAS = (
    "Control",
    "Con falla",
    "Con corrección",
)


def cargar_datos():
    """Carga las métricas consolidadas."""

    if not ARCHIVO_CSV.exists():
        raise FileNotFoundError(
            "No se encontró el CSV consolidado. "
            "Ejecuta primero: "
            "python -m experimentos.consolidar"
        )

    datos = {}

    with ARCHIVO_CSV.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as archivo:

        lector = csv.DictReader(archivo)

        for fila in lector:

            experimento = fila["experimento"]
            fase = fila["fase"]

            datos.setdefault(
                experimento,
                {}
            )[fase] = fila

    return datos


def obtener_valores(datos, experimento, metrica):
    """Obtiene los valores de una métrica."""

    valores = []

    for fase in FASES:

        valor = datos[experimento][fase].get(
            metrica
        )

        if valor in (None, ""):
            raise ValueError(
                f"Métrica no disponible: "
                f"{experimento} / {fase} / {metrica}"
            )

        valores.append(float(valor))

    return valores


def guardar_grafica(nombre):
    """Guarda la figura actual."""

    GRAFICAS.mkdir(
        parents=True,
        exist_ok=True
    )

    ruta = GRAFICAS / nombre

    plt.tight_layout()

    plt.savefig(
        ruta,
        dpi=200,
        bbox_inches="tight"
    )

    plt.close()

    print("Generada:", ruta)


def graficar_metrica(
    datos,
    experimento,
    metrica,
    titulo,
    eje_y,
    nombre
):
    """
    Genera una gráfica de barras.

    Utiliza los valores de las medianas consolidadas.
    """

    valores = obtener_valores(
        datos,
        experimento,
        metrica
    )

    plt.figure(figsize=(9, 5))

    barras = plt.bar(
        ETIQUETAS,
        valores
    )

    plt.title(titulo)

    plt.ylabel(eje_y)

    plt.grid(
        axis="y",
        alpha=0.25
    )

    for barra, valor in zip(barras, valores):

        plt.text(
            barra.get_x() + barra.get_width() / 2,
            barra.get_height(),
            f"{valor:,.2f}",
            ha="center",
            va="bottom",
            fontsize=9
        )

    limite = max(valores) if valores else 0

    plt.ylim(
        0,
        limite * 1.18 if limite > 0 else 1
    )

    guardar_grafica(nombre)


def generar():

    datos = cargar_datos()

    print("=" * 65)
    print("SUPPLYGRID - GRÁFICAS EXPERIMENTALES")
    print("=" * 65)

    configuraciones = [
        (
            "EXP-01",
            "p95_ms",
            "EXP-01 - Latencia p95",
            "Latencia p95 (ms)",
            "EXP-01_p95.png",
        ),
        (
            "EXP-01",
            "throughput_ops_s",
            "EXP-01 - Throughput",
            "Operaciones exitosas por segundo",
            "EXP-01_throughput.png",
        ),
        (
            "EXP-02",
            "disponibilidad_pct",
            "EXP-02 - Disponibilidad",
            "Disponibilidad (%)",
            "EXP-02_disponibilidad.png",
        ),
        (
            "EXP-02",
            "errores",
            "EXP-02 - Errores",
            "Errores por 200 solicitudes",
            "EXP-02_errores.png",
        ),
        (
            "EXP-03",
            "p95_ms",
            "EXP-03 - Latencia p95",
            "Latencia p95 (ms)",
            "EXP-03_p95.png",
        ),
        (
            "EXP-03",
            "throughput_ops_s",
            "EXP-03 - Throughput",
            "Operaciones exitosas por segundo",
            "EXP-03_throughput.png",
        ),
        (
            "EXP-03",
            "cpu_proceso_pct",
            "EXP-03 - Utilización CPU del proceso",
            "CPU del proceso (%)",
            "EXP-03_cpu.png",
        ),
        (
            "EXP-04",
            "disponibilidad_pct",
            "EXP-04 - Disponibilidad",
            "Disponibilidad (%)",
            "EXP-04_disponibilidad.png",
        ),
        (
            "EXP-04",
            "errores",
            "EXP-04 - Errores",
            "Errores por 200 solicitudes",
            "EXP-04_errores.png",
        ),
    ]

    for configuracion in configuraciones:

        graficar_metrica(
            datos,
            *configuracion
        )

    print(
        "\nTotal de gráficas:",
        len(configuraciones)
    )


if __name__ == "__main__":
    generar()
