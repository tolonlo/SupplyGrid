#!/usr/bin/env python3
"""
datos/generar.py — E2-02 (version minima, Tom)

Genera CSVs chiquitos y validos para las 7 tablas, solo para poder
probar la carga y la medicion HOY. Todavia NO tiene el sesgo Zipf real
ni la estacionalidad ni los 8 casos borde con detalle: eso lo completa
Ema en E2-02b sobre este mismo archivo (buscar los comentarios
"E2-02b" mas abajo, ahi va cada reemplazo).

Uso:
    python3 datos/generar.py --seed 42 --out datos/salida

Reglas que SI se respetan desde ya:
  * Escritura por streaming (csv.writer fila a fila), nunca todo en
    memoria.
  * Orden que respeta dependencias: proveedores -> contratos -> catalogo
    -> franjas -> ordenes -> lineas -> eventos.
  * Llaves foraneas resueltas por aritmetica sobre los IDs (nunca
    consultando la base).
  * --seed fijo -> reproducible.
"""
import argparse
import csv
import os
import random
from datetime import datetime, timedelta

# Volumen chico a proposito (version minima). E2-02b lo sube al objetivo
# real del enunciado (~4.9M filas).
N_PROVEEDORES = 2_000
N_CONTRATOS = 300
N_SKU = 5_000
N_CEDIS = 5
N_FRANJAS = 3_000
N_ORDENES = 8_000
LINEAS_PROMEDIO = 5
N_EVENTOS = 4_000


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=str, default="datos/salida")
    return ap.parse_args()


def fecha_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def datetime_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def main():
    args = parse_args()
    random.seed(args.seed)
    os.makedirs(args.out, exist_ok=True)

    hoy = datetime(2026, 9, 18)

    # ---------------------------------------------------------------
    # proveedores.proveedores
    # ---------------------------------------------------------------
    ruta = os.path.join(args.out, "proveedores.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "nombre", "nit", "ciudad", "fecha_registro", "activo"])
        ciudades = ["Medellin", "Bogota", "Cali", "Barranquilla", "Bucaramanga"]
        for i in range(1, N_PROVEEDORES + 1):
            w.writerow([
                i,
                f"Proveedor {i}",
                f"NIT-{900000000 + i}",
                random.choice(ciudades),
                fecha_iso(hoy - timedelta(days=random.randint(0, 1500))),
                "true",
            ])
    print(f"proveedores.csv -> {N_PROVEEDORES} filas")

    # ---------------------------------------------------------------
    # proveedores.contratos (solo una fraccion de proveedores tiene uno)
    # ---------------------------------------------------------------
    ruta = os.path.join(args.out, "contratos.csv")
    contrato_proveedor = {}
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "proveedor_id", "fecha_inicio", "fecha_fin", "estado"])
        proveedores_con_contrato = random.sample(range(1, N_PROVEEDORES + 1), N_CONTRATOS)
        for cid, proveedor_id in enumerate(proveedores_con_contrato, start=1):
            inicio = hoy - timedelta(days=random.randint(30, 700))
            fin = inicio + timedelta(days=random.randint(180, 900))
            w.writerow([cid, proveedor_id, fecha_iso(inicio), fecha_iso(fin), "VIGENTE"])
            contrato_proveedor[cid] = proveedor_id
    print(f"contratos.csv -> {N_CONTRATOS} filas")

    contrato_ids = list(contrato_proveedor.keys())

    # ---------------------------------------------------------------
    # catalogo.catalogo_sku
    # ---------------------------------------------------------------
    ruta = os.path.join(args.out, "catalogo_sku.csv")
    skus_por_contrato = {}
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "proveedor_id", "contrato_id", "sku_codigo",
                     "descripcion", "precio", "fecha_inicio", "fecha_fin"])
        sid = 1
        for _ in range(N_SKU):
            contrato_id = random.choice(contrato_ids)
            proveedor_id = contrato_proveedor[contrato_id]
            sku_codigo = f"SKU-{sid:07d}"
            precio = round(random.uniform(5000, 500000), 2)
            w.writerow([
                sid, proveedor_id, contrato_id, sku_codigo,
                f"Producto {sid}", precio,
                fecha_iso(hoy - timedelta(days=300)),
                fecha_iso(hoy + timedelta(days=300)),
            ])
            skus_por_contrato.setdefault(contrato_id, []).append(sku_codigo)
            sid += 1
    print(f"catalogo_sku.csv -> {N_SKU} filas")

    # ---------------------------------------------------------------
    # logistica.franjas_descargue
    # ---------------------------------------------------------------
    ruta = os.path.join(args.out, "franjas_descargue.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "cedi_id", "fecha", "hora_inicio", "hora_fin",
                     "disponible", "orden_id"])
        for i in range(1, N_FRANJAS + 1):
            cedi_id = random.randint(1, N_CEDIS)
            fecha = hoy + timedelta(days=random.randint(0, 60))
            hora = random.randint(6, 17)
            w.writerow([
                i, cedi_id, fecha_iso(fecha),
                f"{hora:02d}:00:00", f"{hora+1:02d}:00:00",
                "true", "",
            ])
    print(f"franjas_descargue.csv -> {N_FRANJAS} filas")

    # ---------------------------------------------------------------
    # ordenes.ordenes + ordenes.lineas_orden
    # ---------------------------------------------------------------
    ruta_ord = os.path.join(args.out, "ordenes.csv")
    ruta_lin = os.path.join(args.out, "lineas_orden.csv")
    with open(ruta_ord, "w", newline="", encoding="utf-8") as fo, \
         open(ruta_lin, "w", newline="", encoding="utf-8") as fl:
        wo = csv.writer(fo)
        wl = csv.writer(fl)
        wo.writerow(["id", "proveedor_id", "contrato_id", "fecha_orden",
                      "estado", "idempotency_key"])
        wl.writerow(["id", "orden_id", "sku_codigo", "cantidad", "precio_unitario"])

        linea_id = 1
        for orden_id in range(1, N_ORDENES + 1):
            # -----------------------------------------------------------
            # E2-02b (Ema): ACA va el sesgo Zipf. Hoy elegimos el
            # contrato/proveedor de cada orden PAREJO (misma chance para
            # todos) porque solo estamos probando que la tuberia funciona.
            # El reemplazo real es: samplear el proveedor con una
            # distribucion Zipf (parametro s documentado) para que el 5%
            # de proveedores concentre el 40% de las ordenes, y de ahi
            # sacar uno de sus contratos vigentes.
            # -----------------------------------------------------------
            contrato_id = random.choice(contrato_ids)
            proveedor_id = contrato_proveedor[contrato_id]

            fecha = hoy - timedelta(days=random.randint(0, 730),
                                     hours=random.randint(0, 23))
            # -----------------------------------------------------------
            # E2-02b (Ema): ACA va la estacionalidad mensual y horaria
            # (pico fin de mes, pico 8-11am). Hoy la fecha es pareja en
            # todo el rango de 24 meses, sin picos.
            # -----------------------------------------------------------

            wo.writerow([
                orden_id, proveedor_id, contrato_id, datetime_iso(fecha),
                "CONFIRMADA", f"seed{args.seed}-orden-{orden_id}",
            ])
            n_lineas = max(1, int(random.gauss(LINEAS_PROMEDIO, 2)))
            skus_disp = skus_por_contrato.get(contrato_id) or ["SKU-0000001"]
            for _ in range(n_lineas):
                wl.writerow([
                    linea_id, orden_id, random.choice(skus_disp),
                    random.randint(1, 20), round(random.uniform(5000, 500000), 2),
                ])
                linea_id += 1
    print(f"ordenes.csv -> {N_ORDENES} filas")
    print(f"lineas_orden.csv -> {linea_id - 1} filas")

    # ---------------------------------------------------------------
    # auditoria.eventos
    # ---------------------------------------------------------------
    ruta = os.path.join(args.out, "eventos.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "entidad", "entidad_id", "tipo_evento",
                     "fecha_evento", "detalle"])
        for i in range(1, N_EVENTOS + 1):
            w.writerow([
                i, "orden", random.randint(1, N_ORDENES), "ORDEN_CONFIRMADA",
                datetime_iso(hoy - timedelta(days=random.randint(0, 730))),
                "{}",
            ])
    print(f"eventos.csv -> {N_EVENTOS} filas")

    # -----------------------------------------------------------------
    # E2-02b (Ema): ACA faltan los 8 casos borde sembrados a proposito
    # (orden con 300 lineas, contrato vencido ayer, etc.), con sus IDs
    # anotados en un archivo casos-borde.txt.
    # -----------------------------------------------------------------

    print(f"\nListo. seed={args.seed}. Archivos en: {args.out}")


if __name__ == "__main__":
    main()