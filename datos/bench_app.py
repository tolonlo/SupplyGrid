#!/usr/bin/env python3
"""E2-04 — Estrategia 4: cada orden entra por POST /ordenes-compra/{prov}/confirmacion.
Una sola conexion HTTP keep-alive, secuencial (comparable con las otras 3)."""
import csv, sys, http.client
from urllib.parse import urlparse

ruta, base = sys.argv[1], sys.argv[2]
u = urlparse(base)
conn = http.client.HTTPConnection(u.hostname, u.port)
ok = fallos = 0
with open(ruta, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        conn.request(
            "POST",
            f"/ordenes-compra/{r['proveedor_id']}/confirmacion?contratoId={r['contrato_id']}",
            headers={"Idempotency-Key": r["idempotency_key"]},
        )
        resp = conn.getresponse()
        resp.read()
        if resp.status == 200:
            ok += 1
        else:
            fallos += 1
print(f"app: {ok} ok, {fallos} fallos")
sys.exit(1 if fallos else 0)