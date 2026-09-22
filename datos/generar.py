#!/usr/bin/env python3
"""Genera el dataset de carga de SupplyGrid de forma reproducible y parametrizable.

La distribucion de proveedores usa Zipf con exponente s dentro de tres tramos
calibrados: top 1%=--frac-top1, siguiente 4%=(--frac-top5 - --frac-top1) y
resto=lo que queda. Con los valores por defecto (0.74 / 0.15 / 0.40) el
resultado es identico al dataset ya entregado (mismo checksum con --seed 42).

Todo lo que antes era una constante a nivel de modulo (volumen por tabla,
exponente Zipf, dias de historia, fecha de referencia, porcentajes de sesgo)
ahora es un argumento de linea de comandos con ese mismo valor como default.
"""
import argparse
import bisect
import csv
import hashlib
import os
import random
import time
from datetime import datetime, timedelta

# Valores por defecto = exactamente lo que ya se entrego y se referencia
# en el informe (Entrega 2). Cambiarlos vía CLI no afecta el default.
DEFAULTS = {
    "n_proveedores": 100_000,
    "n_contratos": 15_000,
    "n_sku": 400_000,
    "n_cedis": 5,
    "n_franjas": 150_000,
    "n_ordenes": 500_000,
    "n_eventos": 800_000,
    "zipf_s": 0.74,
    "dias_historia": 730,
    "porcion_sku_calientes": 0.10,
    "porcion_lineas_sku_calientes": 0.60,
    "frac_top1": 0.15,
    "frac_top5": 0.40,
    "frac_proveedor_caliente": 0.01,
    "fecha_referencia": "2026-09-18",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Genera el dataset de SupplyGrid (CSV por streaming), "
                     "reproducible con --seed y parametrizable en volumen y sesgo."
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=str, default="datos/salida")

    parser.add_argument("--n-proveedores", type=int, default=DEFAULTS["n_proveedores"],
                         help="Cantidad de proveedores (esquema proveedores)")
    parser.add_argument("--n-contratos", type=int, default=DEFAULTS["n_contratos"],
                         help="Cantidad de contratos (1 por proveedor con contrato)")
    parser.add_argument("--n-sku", type=int, default=DEFAULTS["n_sku"],
                         help="Cantidad de SKU en el catalogo negociado")
    parser.add_argument("--n-cedis", type=int, default=DEFAULTS["n_cedis"],
                         help="Cantidad de CEDI (centros de distribucion)")
    parser.add_argument("--n-franjas", type=int, default=DEFAULTS["n_franjas"],
                         help="Cantidad de franjas de descargue")
    parser.add_argument("--n-ordenes", type=int, default=DEFAULTS["n_ordenes"],
                         help="Cantidad de ordenes de compra (24 meses de historia)")
    parser.add_argument("--n-eventos", type=int, default=DEFAULTS["n_eventos"],
                         help="Cantidad de eventos de auditoria")

    parser.add_argument("--zipf-s", type=float, default=DEFAULTS["zipf_s"],
                         help="Exponente s de la distribucion Zipf entre proveedores")
    parser.add_argument("--frac-top1", type=float, default=DEFAULTS["frac_top1"],
                         help="Fraccion de ordenes que concentra el top 1%% de proveedores")
    parser.add_argument("--frac-top5", type=float, default=DEFAULTS["frac_top5"],
                         help="Fraccion ACUMULADA de ordenes que concentra el top 5%% de proveedores")
    parser.add_argument("--frac-proveedor-caliente", type=float, default=DEFAULTS["frac_proveedor_caliente"],
                         help="Fraccion de ordenes forzada sobre UN solo proveedor (id=3, caso borde 'hot')")

    parser.add_argument("--dias-historia", type=int, default=DEFAULTS["dias_historia"],
                         help="Dias de historia de ordenes hacia atras desde --fecha-referencia")
    parser.add_argument("--fecha-referencia", type=str, default=DEFAULTS["fecha_referencia"],
                         help="Fecha 'hoy' de referencia, formato YYYY-MM-DD")

    parser.add_argument("--porcion-sku-calientes", type=float, default=DEFAULTS["porcion_sku_calientes"],
                         help="Fraccion de SKU considerados 'calientes'")
    parser.add_argument("--porcion-lineas-sku-calientes", type=float, default=DEFAULTS["porcion_lineas_sku_calientes"],
                         help="Fraccion de lineas de orden que usan SKU calientes")

    return parser.parse_args()


def validar_parametros(args):
    """Guarda contra combinaciones que rompen los 8 casos borde exigidos.

    Los casos borde estan atados a IDs concretos (proveedor 3 como 'hot',
    proveedor sin contrato = el ultimo proveedor, orden 1 con 300 lineas,
    etc.). Si alguien reduce el volumen demasiado, estas reglas dejan de
    tener sentido; preferimos fallar temprano con un mensaje claro.
    """
    errores = []
    if args.n_contratos < 3:
        errores.append("--n-contratos debe ser >= 3 (el proveedor 3 es el caso borde 'caliente').")
    if args.n_proveedores <= args.n_contratos:
        errores.append(
            "--n-proveedores debe ser > --n-contratos "
            "(se necesita al menos un proveedor SIN contrato para el CASO-05)."
        )
    if args.n_sku < args.n_contratos:
        errores.append("--n-sku debe ser >= --n-contratos (cada contrato necesita al menos un SKU).")
    if args.n_ordenes < 296:
        errores.append("--n-ordenes debe ser >= 296 para sembrar los tres tramos de lineas por orden (300 / 5 / 6).")
    if args.n_franjas < 1:
        errores.append("--n-franjas debe ser >= 1.")
    if not (0 < args.frac_top1 < args.frac_top5 < 1):
        errores.append("Se requiere 0 < --frac-top1 < --frac-top5 < 1.")
    if errores:
        raise SystemExit("Parametros invalidos:\n- " + "\n- ".join(errores))


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


def construir_pesos_dias(hoy, dias_historia):
    fechas = [hoy - timedelta(days=offset) for offset in range(dias_historia + 1)]
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


def construir_proveedores_calibrados(rng, n_contratos, n_ordenes, zipf_s, frac_top1, frac_top5, frac_proveedor_caliente):
    top_1 = max(3, n_contratos // 100)
    top_5 = max(top_1 + 1, n_contratos // 20)

    def repartir(total, proveedores, pesos):
        total = max(0, total)
        proveedores = list(proveedores)
        cuotas = [total * peso / sum(pesos) for peso in pesos]
        cantidades = [int(cuota) for cuota in cuotas]
        for indice in sorted(range(len(cuotas)), key=lambda i: cuotas[i] - cantidades[i], reverse=True)[:total - sum(cantidades)]:
            cantidades[indice] += 1
        resultado = [proveedor for proveedor, cantidad in zip(proveedores, cantidades) for _ in range(cantidad)]
        rng.shuffle(resultado)
        return resultado

    # El proveedor 3 concentra exactamente frac_proveedor_caliente * n_ordenes
    # (caso borde 7, "proveedor hot"). El resto del top 1% (frac_top1 menos
    # esta fraccion) se reparte por Zipf entre los demas proveedores calientes.
    ordenes_top1_total = int(n_ordenes * frac_top1)
    ordenes_proveedor_caliente = max(1, min(ordenes_top1_total, int(n_ordenes * frac_proveedor_caliente)))

    proveedores_calientes = [proveedor for proveedor in range(1, top_1 + 1) if proveedor != 3]
    pesos_calientes = [max(rango ** -zipf_s, 0.5) for rango in range(1, top_1 + 1) if rango != 3]

    proveedores = [3] * ordenes_proveedor_caliente
    proveedores += repartir(ordenes_top1_total - ordenes_proveedor_caliente, proveedores_calientes, pesos_calientes)
    proveedores += repartir(int(n_ordenes * (frac_top5 - frac_top1)), range(top_1 + 1, top_5 + 1), [1] * (top_5 - top_1))
    proveedores += repartir(n_ordenes - len(proveedores), range(top_5 + 1, n_contratos + 1), [1] * (n_contratos - top_5))
    rng.shuffle(proveedores)
    return proveedores, ordenes_proveedor_caliente


def calcular_lineas_esperadas(n_ordenes):
    """Total de lineas que produce la regla orden 1->300, 2..295->5, resto->6."""
    if n_ordenes <= 0:
        return 0
    lineas = 300
    resto = n_ordenes - 1
    tramo_medio = max(0, min(resto, 294))
    lineas += tramo_medio * 5
    tramo_final = max(0, resto - tramo_medio)
    lineas += tramo_final * 6
    return lineas


def escribir_casos_borde(out, n_proveedores, n_contratos, ordenes_proveedor_caliente, mes_sin_ordenes):
    casos = [
        ("CASO-01 orden=1 lineas=300", "Una orden de 300 lineas rompe supuestos de tamano de transaccion y payload."),
        ("CASO-02 contrato=1", "Un contrato vencido ayer reproduce lecturas obsoletas y carreras durante la validacion de vigencia."),
        ("CASO-03 contrato=2 vence=mediodia", "Un contrato que vence hoy al mediodia reproduce la frontera temporal de la validacion."),
        ("CASO-04 franja=1 cedi=1 ultima=17:00", "La ultima franja disponible del dia reproduce concurrencia sobre un recurso escaso."),
        (f"CASO-05 proveedor={n_proveedores}", "Un proveedor sin contrato vigente reproduce el camino de rechazo."),
        ("CASO-06 sku=SKU-EDGE-FUERA-CATALOGO", "Un SKU fuera del catalogo negociado del proveedor reproduce el camino de rechazo."),
        (f"CASO-07 proveedor=3 ordenes>={ordenes_proveedor_caliente}", "Un proveedor hot reproduce una particion sesgada y presion sobre la cache."),
        (f"CASO-08 proveedor={n_contratos} mes={mes_sin_ordenes}", "Un mes sin ordenes para un proveedor activo rompe promedios y reportes OTIF."),
    ]
    with open(os.path.join(out, "casos-borde.txt"), "w", encoding="utf-8", newline="") as archivo:
        archivo.write("# IDs sembrados y patologia que reproducen\n")
        for identificador, explicacion in casos:
            archivo.write(f"{identificador}: {explicacion}\n")


def main():
    args = parse_args()
    validar_parametros(args)

    rng = random.Random(args.seed)
    os.makedirs(args.out, exist_ok=True)
    hoy = datetime.strptime(args.fecha_referencia, "%Y-%m-%d")

    n_proveedores = args.n_proveedores
    n_contratos = args.n_contratos
    n_sku = args.n_sku
    n_cedis = args.n_cedis
    n_franjas = args.n_franjas
    n_ordenes = args.n_ordenes
    n_eventos = args.n_eventos

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
        for proveedor_id in range(1, n_proveedores + 1):
            writer.writerow([proveedor_id, f"Proveedor {proveedor_id}", f"NIT-{900000000 + proveedor_id}", rng.choice(ciudades), fecha_iso(hoy - timedelta(days=rng.randint(0, 1500))), "true"])
    registrar_archivo(ruta, n_proveedores, inicio_archivo)

    ruta = os.path.join(args.out, "contratos.csv")
    inicio_archivo = comenzar_archivo()
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "proveedor_id", "fecha_inicio", "fecha_fin", "estado"])
        for contrato_id in range(1, n_contratos + 1):
            inicio = hoy - timedelta(days=rng.randint(30, 700))
            fin = hoy + timedelta(days=rng.randint(180, 900))
            estado = "VIGENTE"
            if contrato_id == 1:
                fin, estado = hoy - timedelta(days=1), "VENCIDO"
            elif contrato_id == 2:
                fin = hoy
            fecha_fin = fin.replace(hour=12) if contrato_id == 2 else fin
            writer.writerow([contrato_id, contrato_id, datetime_iso(inicio), datetime_iso(fecha_fin), estado])
    registrar_archivo(ruta, n_contratos, inicio_archivo)

    ruta = os.path.join(args.out, "catalogo_sku.csv")
    inicio_archivo = comenzar_archivo()
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "proveedor_id", "contrato_id", "sku_codigo", "descripcion", "precio", "fecha_inicio", "fecha_fin"])
        for sku_id in range(1, n_sku + 1):
            contrato_id = ((sku_id - 1) % n_contratos) + 1
            sku_codigo = f"SKU-{sku_id:07d}"
            skus_por_contrato.setdefault(contrato_id, []).append(sku_codigo)
            destino = skus_calientes_por_contrato if sku_id <= n_sku * args.porcion_sku_calientes else skus_frios_por_contrato
            destino.setdefault(contrato_id, []).append(sku_codigo)
            writer.writerow([sku_id, contrato_id, contrato_id, sku_codigo, f"Producto {sku_id}", f"{rng.uniform(5000, 500000):.2f}", fecha_iso(hoy - timedelta(days=300)), fecha_iso(hoy + timedelta(days=300))])
    registrar_archivo(ruta, n_sku, inicio_archivo)

    # Cada contrato debe tener al menos un SKU frio y uno caliente para que
    # las lineas de orden siempre puedan elegir de ambas bolsas.
    for contrato_id, skus in skus_por_contrato.items():
        skus_calientes_por_contrato.setdefault(contrato_id, skus[:1])
        skus_frios_por_contrato.setdefault(contrato_id, skus[-1:])

    ruta = os.path.join(args.out, "franjas_descargue.csv")
    inicio_archivo = comenzar_archivo()
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "cedi_id", "fecha", "hora_inicio", "hora_fin", "disponible", "orden_id"])
        for franja_id in range(1, n_franjas + 1):
            if franja_id == 1:
                fecha, cedi, hora = hoy + timedelta(days=1), 1, 17
            else:
                fecha, cedi, hora = hoy + timedelta(days=rng.randint(0, 60)), rng.randint(1, n_cedis), rng.randint(6, 17)
            writer.writerow([franja_id, cedi, fecha_iso(fecha), f"{hora:02d}:00:00", f"{hora + 1:02d}:00:00", "true", ""])
    registrar_archivo(ruta, n_franjas, inicio_archivo)

    proveedores_orden, ordenes_proveedor_caliente = construir_proveedores_calibrados(
        rng, n_contratos, n_ordenes, args.zipf_s, args.frac_top1, args.frac_top5, args.frac_proveedor_caliente
    )
    fechas, acumulada_dias, total_dias = construir_pesos_dias(hoy, args.dias_historia)
    acumulada_horas, total_horas = construir_pesos_horas()

    mes_anterior = hoy.replace(day=1) - timedelta(days=1)
    mes_sin_ordenes = mes_anterior.strftime("%Y-%m")

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
        for orden_id in range(1, n_ordenes + 1):
            contrato_id = proveedores_orden[orden_id - 1]
            fecha = elegir_fecha(rng, fechas, acumulada_dias, total_dias, acumulada_horas, total_horas)
            if contrato_id == n_contratos and fecha.strftime("%Y-%m") == mes_sin_ordenes:
                contrato_id = n_contratos - 1
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
                elif rng.random() < args.porcion_lineas_sku_calientes:
                    sku = rng.choice(skus_calientes_por_contrato[contrato_id])
                else:
                    sku = rng.choice(skus_frios_por_contrato[contrato_id])
                cantidad = rng.randint(1, 20)
                writer_linea.writerow([linea_id, orden_id, sku, cantidad, f"{rng.uniform(5000, 500000):.2f}"])
                linea_id += 1
    registrar_archivo(ruta_ord, n_ordenes, inicio_ordenes)
    registrar_archivo(ruta_lin, linea_id - 1, inicio_ordenes)
    with open(os.path.join(args.out, "checksum_ordenes.txt"), "w", encoding="utf-8") as archivo:
        archivo.write(checksum.hexdigest() + "\n")

    ruta = os.path.join(args.out, "eventos.csv")
    inicio_archivo = comenzar_archivo()
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["id", "entidad", "entidad_id", "tipo_evento", "fecha_evento", "detalle"])
        for evento_id in range(1, n_eventos + 1):
            writer.writerow([evento_id, "orden", rng.randint(1, n_ordenes), "ORDEN_CONFIRMADA", datetime_iso(hoy - timedelta(days=rng.randint(0, 730))), "{}"])
    registrar_archivo(ruta, n_eventos, inicio_archivo)

    escribir_casos_borde(args.out, n_proveedores, n_contratos, ordenes_proveedor_caliente, mes_sin_ordenes)

    lineas_generadas = linea_id - 1
    lineas_esperadas = calcular_lineas_esperadas(n_ordenes)
    if lineas_generadas != lineas_esperadas:
        raise RuntimeError(f"Se esperaban {lineas_esperadas} lineas, se generaron {lineas_generadas}")

    with open(os.path.join(args.out, "metricas_generacion.csv"), "w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["archivo", "filas", "segundos", "bytes"])
        writer.writerows((nombre, filas, f"{segundos:.6f}", bytes_archivo) for nombre, filas, segundos, bytes_archivo in metricas)

    filas_totales = n_proveedores + n_contratos + n_sku + n_franjas + n_ordenes + lineas_generadas + n_eventos
    print(f"filas totales -> {filas_totales}; Zipf s={args.zipf_s}, top 1%={args.frac_top1}, top 5%={args.frac_top5}")
    print(f"Listo. seed={args.seed}. Archivos en: {args.out}")


if __name__ == "__main__":
    main()