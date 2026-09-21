#!/usr/bin/env python3
"""E2-04 — Estrategia 4: órdenes mediante el endpoint real.

Cada orden pasa por las validaciones reales:
- contrato vigente
- SKU negociado vigente
- reserva de franja
- idempotencia

Se utiliza una conexión HTTP keep-alive y ejecución secuencial.
"""

import csv
import json
import sys
import http.client
from collections import defaultdict
from urllib.parse import urlparse


if len(sys.argv) < 3:
    print("Uso: python3 datos/bench_app.py <ordenes.csv> <base_url>")
    sys.exit(1)


ruta_ordenes = sys.argv[1]
base_url = sys.argv[2]

u = urlparse(base_url)

conn = http.client.HTTPConnection(
    u.hostname,
    u.port or 80,
    timeout=30
)


# ------------------------------------------------------------
# Cargar un SKU real asociado a cada orden
# ------------------------------------------------------------

sku_por_orden = {}

with open(
    "datos/salida/lineas_orden.csv",
    newline="",
    encoding="utf-8"
) as f:

    for linea in csv.DictReader(f):
        orden_id = linea["orden_id"]

        # Para validar el endpoint basta con un SKU valido de la orden.
        sku = linea["sku_codigo"]

        # El dataset incluye deliberadamente este SKU inválido como caso borde
        # de E2-03b. No debe utilizarse como entrada válida del benchmark E2-04.
        if sku == "SKU-EDGE-FUERA-CATALOGO":
            continue

        if orden_id not in sku_por_orden:
            sku_por_orden[orden_id] = sku


# ------------------------------------------------------------
# Franjas disponibles
#
# Se utilizan pares CEDI/fecha existentes en el dataset.
# Cada combinación tiene cientos de franjas disponibles.
# ------------------------------------------------------------

destinos = [
    (5, "2026-10-10"),
    (1, "2026-10-04"),
    (4, "2026-10-28"),
    (2, "2026-09-25"),
    (2, "2026-10-12"),
    (4, "2026-11-17"),
    (5, "2026-09-25"),
    (1, "2026-09-23"),
    (3, "2026-10-13"),
    (5, "2026-11-15"),
    (3, "2026-09-24"),
]


ok = 0
fallos = 0
errores = defaultdict(int)


# ------------------------------------------------------------
# Ejecutar órdenes por el endpoint real
# ------------------------------------------------------------

with open(ruta_ordenes, newline="", encoding="utf-8") as f:

    for indice, orden in enumerate(csv.DictReader(f)):

        orden_id = orden["id"]

        sku = sku_por_orden.get(orden_id)

        if sku is None:
            fallos += 1
            errores["sin_sku"] += 1
            continue

        cedi_id, fecha_descargue = destinos[indice % len(destinos)]

        body = json.dumps({
            "skus": [sku],
            "cediId": cedi_id,
            "fechaDescargue": fecha_descargue
        })

        try:
            conn.request(
                "POST",
                (
                    f"/ordenes-compra/"
                    f"{orden['proveedor_id']}/confirmacion"
                    f"?contratoId={orden['contrato_id']}"
                ),
                body=body,
                headers={
                    "Idempotency-Key": orden["idempotency_key"],
                    "Content-Type": "application/json"
                },
            )

            resp = conn.getresponse()
            respuesta = resp.read().decode(
                "utf-8",
                errors="replace"
            )

            if resp.status == 200:
                ok += 1
            else:
                fallos += 1
                errores[f"HTTP_{resp.status}"] += 1

                # Mostrar solo los primeros errores para no llenar terminal.
                if fallos <= 10:
                    print(
                        f"FALLO orden={orden_id} "
                        f"HTTP={resp.status}: {respuesta}"
                    )

        except Exception as exc:
            fallos += 1
            errores["conexion"] += 1

            if fallos <= 10:
                print(
                    f"ERROR orden={orden_id}: {exc}"
                )

            # Reabrir conexión en caso de fallo HTTP.
            conn.close()

            conn = http.client.HTTPConnection(
                u.hostname,
                u.port or 80,
                timeout=30
            )


conn.close()


print()
print("=== RESULTADO ENDPOINT REAL ===")
print(f"OK:     {ok}")
print(f"Fallos: {fallos}")

if errores:
    print("Detalle:")
    for tipo, cantidad in errores.items():
        print(f"  {tipo}: {cantidad}")


sys.exit(1 if fallos else 0)