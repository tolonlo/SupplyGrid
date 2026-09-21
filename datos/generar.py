#!/usr/bin/env python3
"""Genera el dataset de carga de SupplyGrid de forma reproducible.

La distribucion de proveedores usa Zipf con s=0.74 dentro de tres tramos
calibrados: top 1%=15%, siguiente 4%=25% y resto=60%. Asi se conservan los
parametros documentados y las concentraciones exigidas por el escenario.
"""
import argparse
import bisect
import csv
import hashlib
import os
import random
import time
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
PORCION_SKU_CALIENTES = 0.10
PORCION_LINEAS_SKU_CALIENTES = 0.60


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


def ultimo_dia_mes(fecha):
    siguiente = fecha.replace(day=28) + timedelta(days=4)
    return siguiente - timedelta(days=siguiente.day)


def es_ultimo_dia_habil(fecha):
    ultimo = ultimo_dia_mes(fecha)
    dias_habiles = []
    cursor = ultimo
    while len(dias_habiles) < 3:
        if cursor.weekday() < 5:
            dias_habiles.append(cursor.date())
        cursor -= timedelta(days=1)
    return fecha.date() in dias_habiles


def construir_pesos_dias(hoy):
    fechas = [hoy - timedelta(days=offset) for offset in range(DIAS_HISTORIA + 1)]
    especiales = [es_ultimo_dia_habil(fecha) for fecha in fechas]
    total_especiales = sum(especiales)
    total_normales = len(fechas) - total_especiales
    pesos = [
        (0.25 / total_especiales) if especial else (0.75 / total_normales)
        for especial in especiales
    ]
    acumulada = []
    total = 0
    for peso in pesos:
        total += peso
        acumulada.append(total)
    return fechas, acumulada, total


def construir_pesos_horas():
    pesos = []
    for hora in range(24):
        if 8 <= hora <= 11:
            pesos.append(0.25 / 4)
        elif hora <= 5 or hora >= 22:
            pesos.append(0.04 / 8)
        else:
            pesos.append(0.71 / 12)
    acumulada = []
    total = 0.0
    for peso in pesos:
        total += peso
        acumulada.append(total)
    return acumulada, total


def elegir_fecha(rng, fechas, acumulada_dias, total_dias, acumulada_horas, total_horas):
    indice = bisect.bisect_left(acumulada_dias, rng.random() * total_dias)
    hora = bisect.bisect_left(acumulada_horas, rng.random() * total_horas)
    return fechas[indice].replace(hour=hora, minute=rng.randrange(60), second=rng.randrange(60))


def construir_proveedores_calibrados(rng):
    top_1 = N_CONTRATOS // 100
    top_5 = N_CONTRATOS // 20
    def repartir(total, proveedores, pesos):
        cuotas = [total * peso / sum(pesos) for peso in pesos]
        cantidades = [int(cuota) for cuota in cuotas]
        for indice in sorted(range(len(cuotas)), key=lambda i: cuotas[i] - cantidades[i], reverse=True)[:total - sum(cantidades)]:
            cantidades[indice] += 1
        resultado = [proveedor for proveedor, cantidad in zip(proveedores, cantidades) for _ in range(cantidad)]
        rng.shuffle(resultado)
        return resultado

    # El piso evita que la cola del tramo caliente caiga por debajo del
    # promedio del tramo siguiente al ordenar por cantidad de ordenes.
    proveedores_calientes = [proveedor for proveedor in range(1, top_1 + 1) if proveedor != 3]
    pesos_calientes = [max(rango ** -ZIPF_S, 0.5) for rango in range(1, top_1 + 1) if rango != 3]
    proveedores = [3] * 5_000 + repartir(int(N_ORDENES * 0.15) - 5_000, proveedores_calientes, pesos_calientes)
    proveedores += repartir(int(N_ORDENES * 0.25), range(top_1 + 1, top_5 + 1), [1] * (top_5 - top_1))
    proveedores += repartir(N_ORDENES - len(proveedores), range(top_5 + 1, N_CONTRATOS + 1), [1] * (N_CONTRATOS - top_5))
    rng.shuffle(proveedores)
    return proveedores


def escribir_casos_borde(out):
    casos = [
        ("CASO-01 orden=1 lineas=300", "Una orden de 300 lineas rompe supuestos de tamano de transaccion y payload."),
        ("CASO-02 contrato=1", "Un contrato vencido ayer reproduce lecturas obsoletas y carreras durante la validacion de vigencia."),
        ("CASO-03 contrato=2 vence=mediodia", "Un contrato que vence hoy al mediodia reproduce la frontera temporal de la validacion."),
        ("CASO-04 franja=1 cedi=1 ultima=17:00", "La ultima franja disponible del dia reproduce concurrencia sobre un recurso escaso."),
        ("CASO-05 proveedor=100000", "Un proveedor sin contrato vigente reproduce el camino de rechazo."),
        ("CASO-06 sku=SKU-EDGE-FUERA-CATALOGO", "Un SKU fuera del catalogo negociado del proveedor reproduce el camino de rechazo."),
        ("CASO-07 proveedor=3 ordenes>=5000", "Un proveedor hot reproduce una particion sesgada y presion sobre la cache."),
        ("CASO-08 proveedor=15000 mes=2026-08", "Un mes sin ordenes para un proveedor activo rompe promedios y reportes OTIF."),
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
    skus_por_contrato = {}
    skus_calientes_por_contrato = {}
    skus_frios_por_contrato = {}
    metricas = []

    def comenzar_archivo():
        return time.perf_counter()

    def registrar_archivo(ruta, filas, inicio):
        metricas.append((os.path.basename(ruta), filas, time.perf_counter() - inicio, os.path.getsize(ruta)))

    ruta = os.path.join(args.out, "proveedores.csv")
    inicio_archivo = comenzar_archivo()
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "nombre", "nit", "ciudad", "fecha_registro", "activo"])
        ciudades = ["Medellin", "Bogota", "Cali", "Barranquilla", "Bucaramanga"]
        for proveedor_id in range(1, N_PROVEEDORES + 1):
            writer.writerow([proveedor_id, f"Proveedor {proveedor_id}", f"NIT-{900000000 + proveedor_id}", rng.choice(ciudades), fecha_iso(hoy - timedelta(days=rng.randint(0, 1500))), "true"])
    registrar_archivo(ruta, N_PROVEEDORES, inicio_archivo)

    ruta = os.path.join(args.out, "contratos.csv")
    inicio_archivo = comenzar_archivo()
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
            fecha_fin = fin.replace(hour=12) if contrato_id == 2 else fin
            writer.writerow([contrato_id, contrato_id, datetime_iso(inicio), datetime_iso(fecha_fin), estado])
    registrar_archivo(ruta, N_CONTRATOS, inicio_archivo)

    ruta = os.path.join(args.out, "catalogo_sku.csv")
    inicio_archivo = comenzar_archivo()
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "proveedor_id", "contrato_id", "sku_codigo", "descripcion", "precio", "fecha_inicio", "fecha_fin"])
        for sku_id in range(1, N_SKU + 1):
            contrato_id = ((sku_id - 1) % N_CONTRATOS) + 1
            sku_codigo = f"SKU-{sku_id:07d}"
            skus_por_contrato.setdefault(contrato_id, []).append(sku_codigo)
            destino = skus_calientes_por_contrato if sku_id <= N_SKU * PORCION_SKU_CALIENTES else skus_frios_por_contrato
            destino.setdefault(contrato_id, []).append(sku_codigo)
            writer.writerow([sku_id, contrato_id, contrato_id, sku_codigo, f"Producto {sku_id}", f"{rng.uniform(5000, 500000):.2f}", fecha_iso(hoy - timedelta(days=300)), fecha_iso(hoy + timedelta(days=300))])
    registrar_archivo(ruta, N_SKU, inicio_archivo)

    ruta = os.path.join(args.out, "franjas_descargue.csv")
    inicio_archivo = comenzar_archivo()
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "cedi_id", "fecha", "hora_inicio", "hora_fin", "disponible", "orden_id"])
        for franja_id in range(1, N_FRANJAS + 1):
            if franja_id == 1:
                fecha, cedi, hora = hoy + timedelta(days=1), 1, 17
            else:
                fecha, cedi, hora = hoy + timedelta(days=rng.randint(0, 60)), rng.randint(1, N_CEDIS), rng.randint(6, 17)
            writer.writerow([franja_id, cedi, fecha_iso(fecha), f"{hora:02d}:00:00", f"{hora + 1:02d}:00:00", "true", ""])
    registrar_archivo(ruta, N_FRANJAS, inicio_archivo)

    proveedores_orden = construir_proveedores_calibrados(rng)
    fechas, acumulada_dias, total_dias = construir_pesos_dias(hoy)
    acumulada_horas, total_horas = construir_pesos_horas()
    mes_sin_ordenes = "2026-08"
    ruta_ord = os.path.join(args.out, "ordenes.csv")
    ruta_lin = os.path.join(args.out, "lineas_orden.csv")
    inicio_ordenes = comenzar_archivo()
    checksum = hashlib.md5()
    with open(ruta_ord, "w", newline="", encoding="utf-8") as ordenes, open(ruta_lin, "w", newline="", encoding="utf-8") as lineas:
        writer_orden = csv.writer(ordenes)
        writer_linea = csv.writer(lineas)
        writer_orden.writerow(["id", "proveedor_id", "contrato_id", "fecha_orden", "estado", "idempotency_key"])
        writer_linea.writerow(["id", "orden_id", "sku_codigo", "cantidad", "precio_unitario"])
        linea_id = 1
        for orden_id in range(1, N_ORDENES + 1):
            contrato_id = proveedores_orden[orden_id - 1]
            fecha = elegir_fecha(rng, fechas, acumulada_dias, total_dias, acumulada_horas, total_horas)
            if contrato_id == 15000 and fecha.strftime("%Y-%m") == mes_sin_ordenes:
                contrato_id = 14999
            idempotency_key = f"seed{args.seed}-orden-{orden_id}"
            checksum.update(idempotency_key.encode("utf-8"))
            writer_orden.writerow([orden_id, contrato_id, contrato_id, datetime_iso(fecha), "CONFIRMADA", idempotency_key])
            if orden_id == 1:
                cantidad_lineas = 300
            elif orden_id <= 295:
                cantidad_lineas = 5
            else:
                cantidad_lineas = 6
            for indice in range(cantidad_lineas):
                if orden_id == 3 and indice == 0:
                    sku = "SKU-EDGE-FUERA-CATALOGO"
                elif rng.random() < PORCION_LINEAS_SKU_CALIENTES:
                    sku = rng.choice(skus_calientes_por_contrato[contrato_id])
                else:
                    sku = rng.choice(skus_frios_por_contrato[contrato_id])
                cantidad = rng.randint(1, 20)
                writer_linea.writerow([linea_id, orden_id, sku, cantidad, f"{rng.uniform(5000, 500000):.2f}"])
                linea_id += 1
    registrar_archivo(ruta_ord, N_ORDENES, inicio_ordenes)
    registrar_archivo(ruta_lin, linea_id - 1, inicio_ordenes)
    with open(os.path.join(args.out, "checksum_ordenes.txt"), "w", encoding="utf-8") as archivo:
        archivo.write(checksum.hexdigest() + "\n")

    ruta = os.path.join(args.out, "eventos.csv")
    inicio_archivo = comenzar_archivo()
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "entidad", "entidad_id", "tipo_evento", "fecha_evento", "detalle"])
        for evento_id in range(1, N_EVENTOS + 1):
            writer.writerow([evento_id, "orden", rng.randint(1, N_ORDENES), "ORDEN_CONFIRMADA", datetime_iso(hoy - timedelta(days=rng.randint(0, 730))), "{}"])
    registrar_archivo(ruta, N_EVENTOS, inicio_archivo)

    escribir_casos_borde(args.out)
    lineas_generadas = linea_id - 1
    if lineas_generadas != N_LINEAS_OBJETIVO:
        raise RuntimeError(f"Se esperaban {N_LINEAS_OBJETIVO} lineas, se generaron {lineas_generadas}")
    with open(os.path.join(args.out, "metricas_generacion.csv"), "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["archivo", "filas", "segundos", "bytes"])
        writer.writerows((nombre, filas, f"{segundos:.6f}", bytes_archivo) for nombre, filas, segundos, bytes_archivo in metricas)
    filas_totales = N_PROVEEDORES + N_CONTRATOS + N_SKU + N_FRANJAS + N_ORDENES + lineas_generadas + N_EVENTOS
    print(f"filas totales -> {filas_totales}; Zipf s={ZIPF_S}, top 1%=0.15, top 5%=0.40")
    print(f"Listo. seed={args.seed}. Archivos en: {args.out}")


if __name__ == "__main__":
    main()
