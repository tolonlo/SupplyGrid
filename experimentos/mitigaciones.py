import time
from collections import OrderedDict

from chaos import observabilidad as obs


class CircuitoAbierto(Exception):
    """La solicitud se rechaza porque el circuit breaker está abierto."""


class CacheTTL:
    """
    Caché en memoria con expiración y límite de entradas.

    Diseñada para consultas de solo lectura.
    E3: cada acierto o fallo se reporta a observabilidad (métrica chaos_cache_total).
    """

    def __init__(self, ttl_segundos=60, capacidad=1000, nombre="Q1"):
        if ttl_segundos <= 0:
            raise ValueError("El TTL debe ser positivo")

        if capacidad <= 0:
            raise ValueError("La capacidad debe ser positiva")

        self.ttl = ttl_segundos
        self.capacidad = capacidad
        self.nombre = nombre
        self.entradas = OrderedDict()

        self.hits = 0
        self.misses = 0

    def obtener(self, clave):
        ahora = time.monotonic()

        if clave not in self.entradas:
            self.misses += 1
            obs.cache(self.nombre, hit=False)
            return None

        valor, expiracion = self.entradas[clave]

        if ahora >= expiracion:
            del self.entradas[clave]
            self.misses += 1
            obs.cache(self.nombre, hit=False)
            return None

        self.entradas.move_to_end(clave)
        self.hits += 1
        obs.cache(self.nombre, hit=True)

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


class CircuitBreaker:
    """
    Cortacircuitos para fallas transitorias (pool / 503).

    Estados: cerrado → abierto → semiabierto → cerrado.
    Se usa como corrección alternativa a los reintentos ilimitados
    por solicitud: corta la cascada cuando el error deja de ser puntual.
    """

    CERRADO = "cerrado"
    ABIERTO = "abierto"
    SEMIABIERTO = "semiabierto"

    def __init__(
        self,
        umbral_fallos=5,
        timeout_abierto_s=0.2,
        nombre="Q1",
    ):
        if umbral_fallos < 1:
            raise ValueError("El umbral de fallos debe ser >= 1")
        if timeout_abierto_s <= 0:
            raise ValueError("El timeout de apertura debe ser positivo")

        self.umbral_fallos = umbral_fallos
        self.timeout_abierto_s = timeout_abierto_s
        self.nombre = nombre
        self.estado = self.CERRADO
        self.fallos_seguidos = 0
        self.abierto_desde = 0.0
        self.rechazos = 0
        self.aperturas = 0
        self.cierres = 0

    def _puede_probar(self):
        if self.estado != self.ABIERTO:
            return True
        if time.monotonic() - self.abierto_desde >= self.timeout_abierto_s:
            self.estado = self.SEMIABIERTO
            obs.circuito(self.nombre, self.SEMIABIERTO, "timeout_abierto")
            return True
        return False

    def ejecutar(self, fn):
        if not self._puede_probar():
            self.rechazos += 1
            obs.circuito(self.nombre, self.ABIERTO, "rechazo")
            raise CircuitoAbierto(
                f"Circuit breaker abierto para {self.nombre}"
            )
        try:
            resultado = fn()
        except Exception:
            self.fallos_seguidos += 1
            if self.fallos_seguidos >= self.umbral_fallos:
                self.estado = self.ABIERTO
                self.abierto_desde = time.monotonic()
                self.aperturas += 1
                obs.circuito(self.nombre, self.ABIERTO, "umbral")
            raise

        self.fallos_seguidos = 0
        if self.estado == self.SEMIABIERTO:
            self.estado = self.CERRADO
            self.cierres += 1
            obs.circuito(self.nombre, self.CERRADO, "prueba_ok")
        return resultado

    def estadisticas(self):
        return {
            "estado": self.estado,
            "fallos_seguidos": self.fallos_seguidos,
            "rechazos": self.rechazos,
            "aperturas": self.aperturas,
            "cierres": self.cierres,
        }
