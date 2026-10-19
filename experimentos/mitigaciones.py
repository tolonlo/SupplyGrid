import time
from collections import OrderedDict


class CacheTTL:
    """
    Caché en memoria con expiración y límite de entradas.

    Diseñada para consultas de solo lectura.
    """

    def __init__(self, ttl_segundos=60, capacidad=1000):
        if ttl_segundos <= 0:
            raise ValueError("El TTL debe ser positivo")

        if capacidad <= 0:
            raise ValueError("La capacidad debe ser positiva")

        self.ttl = ttl_segundos
        self.capacidad = capacidad
        self.entradas = OrderedDict()

        self.hits = 0
        self.misses = 0

    def obtener(self, clave):
        ahora = time.monotonic()

        if clave not in self.entradas:
            self.misses += 1
            return None

        valor, expiracion = self.entradas[clave]

        if ahora >= expiracion:
            del self.entradas[clave]
            self.misses += 1
            return None

        self.entradas.move_to_end(clave)
        self.hits += 1

        return valor

    def guardar(self, clave, valor):
        expiracion = time.monotonic() + self.ttl

        if clave in self.entradas:
            del self.entradas[clave]

        self.entradas[clave] = (valor, expiracion)

        while len(self.entradas) > self.capacidad:
            self.entradas.popitem(last=False)

    def limpiar(self):
        self.entradas.clear()
        self.hits = 0
        self.misses = 0

    def estadisticas(self):
        total = self.hits + self.misses

        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate_pct": (
                100 * self.hits / total if total else 0
            ),
            "entradas": len(self.entradas)
        }