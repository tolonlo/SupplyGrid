#!/usr/bin/env python3
"""Genera el dataset de carga de SupplyGrid de forma reproducible.

Zipf discreto con s=0.74: en 15.000 proveedores con contrato, los 750
primeros concentran aproximadamente 40% del peso. Las fechas ponderan los tres
ultimos dias de cada mes y las horas 08:00-11:59 para reproducir picos.
"""
import argparse
import bisect
import csv
import os
import random
from datetime import datetime, timedelta

N_PROVEEDORES = 100_000
N_CONTRATOS = 15_000
N_SKU = 400_000
N_CEDIS = 5
N_FRANJAS = 150_000
N_ORDENES = 500_000
N_LINEAS_OBJETIVO = 3_000_000
N_EVENTOS = 800_000
ZIPF_S = 0.74
DIAS_HISTORIA = 730


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=str, default="datos/salida")
    return parser.parse_args()


def fecha_iso(fecha):
    return fecha.strftime("%Y-%m-%d")


def datetime_iso(fecha):
    return fecha.strftime("%Y-%m-%dT%H:%M:%S")


def cdf_zipf(n, exponent):
    acumulada = []
    total = 0.0
    for rango in range(1, n + 1):
        total += rango ** -exponent
        acumulada.append(total)
    return acumulada, total


def elegir_zipf(rng, acumulada, total):
    return bisect.bisect_left(acumulada, rng.random() * total) + 1


def es_fin_de_mes(fecha):
    siguiente = fecha.replace(day=28) + timedelta(days=4)
    ultimo_dia = siguiente - timedelta(days=siguiente.day)
    return fecha.day >= ultimo_dia.day - 2


def construir_pesos_dias(hoy):
    fechas = [hoy - timedelta(days=offset) for offset in range(DIAS_HISTORIA + 1)]
    pesos = [3 if es_fin_de_mes(fecha) else 1 for fecha in fechas]
    acumulada = []
    total = 0
    for peso in pesos:
        total += peso
        acumulada.append(total)
    return fechas, acumulada, total


def construir_pesos_horas():
    acumulada = []
    total = 0
    for hora in range(24):
        total += 3 if 8 <= hora <= 11 else 1
        acumulada.append(total)
    return acumulada, total


def elegir_fecha(rng, fechas, acumulada_dias, total_dias, acumulada_horas, total_horas):
    indice = bisect.bisect_left(acumulada_dias, rng.random() * total_dias)
    hora = bisect.bisect_left(acumulada_horas, rng.random() * total_horas)
    return fechas[indice].replace(hour=hora, minute=rng.randrange(60), second=rng.randrange(60))


def escribir_casos_borde(out):
    casos = [
        ("CASO-01 orden=1 lineas=300", "Una orden de 300 lineas rompe supuestos de tamano de transaccion y payload."),
        ("CASO-02 contrato=1", "Un contrato vencido ayer reproduce lecturas obsoletas y carreras durante la validacion de vigencia."),
        ("CASO-03 contrato=2", "Un contrato que vence hoy reproduce el limite temporal entre disponible y vencido."),
        ("CASO-04 franja=1", "Una franja agotada reproduce contencion concurrente por el ultimo recurso logistico."),
        ("CASO-05 orden=2", "Una orden con 500 unidades reproduce cantidades atipicas que desbordan validaciones y totales."),
        ("CASO-06 linea=307 sku=SKU-EDGE-FUERA-CATALOGO", "Un SKU fuera del catalogo del proveedor reproduce referencias cruzadas invalidas."),
        ("CASO-07 evento=1", "Un evento con detalle grande reproduce crecimiento append-only y presion de almacenamiento."),
        ("CASO-08 proveedor=100000", "Un proveedor sin contrato reproduce altas incompletas y fallos de resolucion entre modulos."),
    ]
    with open(os.path.join(out, "casos-borde.txt"), "w", encoding="utf-8", newline="") as archivo:
        archivo.write("# IDs sembrados y patologia que reproducen\n")
        for identificador, explicacion in casos:
            archivo.write(f"{identificador}: {explicacion}\n")


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    os.makedirs(args.out, exist_ok=True)
    hoy = datetime(2026, 9, 18)
    contrato_proveedor = {}
    skus_por_contrato = {}

    ruta = os.path.join(args.out, "proveedores.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "nombre", "nit", "ciudad", "fecha_registro", "activo"])
        ciudades = ["Medellin", "Bogota", "Cali", "Barranquilla", "Bucaramanga"]
        for proveedor_id in range(1, N_PROVEEDORES + 1):
            writer.writerow([proveedor_id, f"Proveedor {proveedor_id}", f"NIT-{900000000 + proveedor_id}", rng.choice(ciudades), fecha_iso(hoy - timedelta(days=rng.randint(0, 1500))), "true"])

    ruta = os.path.join(args.out, "contratos.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "proveedor_id", "fecha_inicio", "fecha_fin", "estado"])
        for contrato_id in range(1, N_CONTRATOS + 1):
            inicio = hoy - timedelta(days=rng.randint(30, 700))
            fin = hoy + timedelta(days=rng.randint(180, 900))
            estado = "VIGENTE"
            if contrato_id == 1:
                fin, estado = hoy - timedelta(days=1), "VENCIDO"
            elif contrato_id == 2:
                fin = hoy
            writer.writerow([contrato_id, contrato_id, fecha_iso(inicio), fecha_iso(fin), estado])
            contrato_proveedor[contrato_id] = contrato_id

    ruta = os.path.join(args.out, "catalogo_sku.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "proveedor_id", "contrato_id", "sku_codigo", "descripcion", "precio", "fecha_inicio", "fecha_fin"])
        for sku_id in range(1, N_SKU + 1):
            contrato_id = ((sku_id - 1) % N_CONTRATOS) + 1
            sku_codigo = f"SKU-{sku_id:07d}"
            skus_por_contrato.setdefault(contrato_id, []).append(sku_codigo)
            writer.writerow([sku_id, contrato_id, contrato_id, sku_codigo, f"Producto {sku_id}", f"{rng.uniform(5000, 500000):.2f}", fecha_iso(hoy - timedelta(days=300)), fecha_iso(hoy + timedelta(days=300))])

    ruta = os.path.join(args.out, "franjas_descargue.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "cedi_id", "fecha", "hora_inicio", "hora_fin", "disponible", "orden_id"])
        for franja_id in range(1, N_FRANJAS + 1):
            fecha = hoy + timedelta(days=rng.randint(0, 60))
            hora = rng.randint(6, 17)
            writer.writerow([franja_id, rng.randint(1, N_CEDIS), fecha_iso(fecha), f"{hora:02d}:00:00", f"{hora + 1:02d}:00:00", "false" if franja_id == 1 else "true", "1" if franja_id == 1 else ""])

    proveedores_zipf, total_zipf = cdf_zipf(N_CONTRATOS, ZIPF_S)
    fechas, acumulada_dias, total_dias = construir_pesos_dias(hoy)
    acumulada_horas, total_horas = construir_pesos_horas()
    ruta_ord = os.path.join(args.out, "ordenes.csv")
    ruta_lin = os.path.join(args.out, "lineas_orden.csv")
    with open(ruta_ord, "w", newline="", encoding="utf-8") as ordenes, open(ruta_lin, "w", newline="", encoding="utf-8") as lineas:
        writer_orden = csv.writer(ordenes)
        writer_linea = csv.writer(lineas)
        writer_orden.writerow(["id", "proveedor_id", "contrato_id", "fecha_orden", "estado", "idempotency_key"])
        writer_linea.writerow(["id", "orden_id", "sku_codigo", "cantidad", "precio_unitario"])
        linea_id = 1
        for orden_id in range(1, N_ORDENES + 1):
            contrato_id = elegir_zipf(rng, proveedores_zipf, total_zipf)
            fecha = elegir_fecha(rng, fechas, acumulada_dias, total_dias, acumulada_horas, total_horas)
            writer_orden.writerow([orden_id, contrato_id, contrato_id, datetime_iso(fecha), "CONFIRMADA", f"seed{args.seed}-orden-{orden_id}"])
            if orden_id == 1:
                cantidad_lineas = 300
            elif orden_id <= 295:
                cantidad_lineas = 5
            else:
                cantidad_lineas = 6
            for indice in range(cantidad_lineas):
                sku = "SKU-EDGE-FUERA-CATALOGO" if orden_id == 3 and indice == 0 else rng.choice(skus_por_contrato[contrato_id])
                cantidad = 500 if orden_id == 2 and indice == 0 else rng.randint(1, 20)
                writer_linea.writerow([linea_id, orden_id, sku, cantidad, f"{rng.uniform(5000, 500000):.2f}"])
                linea_id += 1

    ruta = os.path.join(args.out, "eventos.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "entidad", "entidad_id", "tipo_evento", "fecha_evento", "detalle"])
        for evento_id in range(1, N_EVENTOS + 1):
            detalle = "{\"payload\":\"" + ("x" * 4096) + "\"}" if evento_id == 1 else "{}"
            writer.writerow([evento_id, "orden", rng.randint(1, N_ORDENES), "ORDEN_CONFIRMADA", datetime_iso(hoy - timedelta(days=rng.randint(0, 730))), detalle])

    escribir_casos_borde(args.out)
    lineas_generadas = linea_id - 1
    if lineas_generadas != N_LINEAS_OBJETIVO:
        raise RuntimeError(f"Se esperaban {N_LINEAS_OBJETIVO} lineas, se generaron {lineas_generadas}")
    filas_totales = N_PROVEEDORES + N_CONTRATOS + N_SKU + N_FRANJAS + N_ORDENES + lineas_generadas + N_EVENTOS
    top_share = sum(rango ** -ZIPF_S for rango in range(1, N_CONTRATOS // 20 + 1)) / total_zipf
    print(f"filas totales -> {filas_totales}; Zipf s={ZIPF_S}, peso teorico top 5%={top_share:.3f}")
    print(f"Listo. seed={args.seed}. Archivos en: {args.out}")


if __name__ == "__main__":
    main()
