"""Pruebas de observabilidad, correlación y circuit breaker (Persona B)."""

import json
import tempfile
import time
import unittest
from pathlib import Path

from chaos import observabilidad as obs
from experimentos.analizar_observabilidad import correlacionar, leer_eventos, resumir
from experimentos.mitigaciones import CacheTTL, CircuitBreaker, CircuitoAbierto


class TestObservabilidad(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.log = str(Path(self.tmp.name) / "eventos.jsonl")
        obs.reiniciar_para_pruebas(self.log)
        obs.iniciar()

    def tearDown(self):
        obs.reiniciar_para_pruebas()
        self.tmp.cleanup()

    def _eventos(self):
        obs.vaciar()
        return leer_eventos(self.log)

    def test_detecta_y_localiza_con_el_mismo_trace_id(self):
        class Falla:
            id = "B1_pool_agotado"

        obs.abrir_fase("EXP-02", "con_falla")

        def boom():
            obs.inyeccion(Falla(), "Q1")
            raise RuntimeError("pool agotado")

        with self.assertRaises(RuntimeError):
            obs.observar_operacion("Q1", boom)

        obs.cerrar_fase()
        eventos = self._eventos()
        cadenas = correlacionar(eventos)
        self.assertGreaterEqual(len(cadenas), 1)
        self.assertEqual(cadenas[0]["fault_id"], "B1_pool_agotado")
        self.assertEqual(cadenas[0]["error"], "RuntimeError")
        self.assertEqual(cadenas[0]["experimento"], "EXP-02")
        self.assertEqual(cadenas[0]["fase"], "con_falla")

    def test_no_registra_operaciones_exitosas_en_modo_errores(self):
        obs.abrir_fase("EXP-01", "control")
        self.assertEqual(obs.observar_operacion("Q1", lambda: "ok"), "ok")
        obs.cerrar_fase()
        tipos = [e["evento"] for e in self._eventos()]
        self.assertNotIn("operacion", tipos)
        self.assertIn("fase_inicio", tipos)
        self.assertIn("fase_fin", tipos)

    def test_kill_switch_queda_en_el_log(self):
        obs.kill_switch("test")
        tipos = [e["evento"] for e in self._eventos()]
        self.assertIn("kill_switch", tipos)

    def test_resumen_separa_senales_utiles_y_poco_utiles(self):
        resumen = resumir(self._eventos())
        self.assertIn("inyeccion", resumen["senales_utiles"])
        self.assertIn("cache_hit", resumen["senales_poco_utiles"])


class TestMitigacionesPersonaB(unittest.TestCase):

    def test_cache_ttl_reporta_hits(self):
        cache = CacheTTL(ttl_segundos=30, capacidad=10, nombre="Q1")
        self.assertIsNone(cache.obtener("a"))
        cache.guardar("a", 1)
        self.assertEqual(cache.obtener("a"), 1)
        stats = cache.estadisticas()
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 1)

    def test_circuit_breaker_abre_y_rechaza(self):
        breaker = CircuitBreaker(umbral_fallos=2, timeout_abierto_s=0.05, nombre="Q1")

        def fallar():
            raise RuntimeError("503")

        with self.assertRaises(RuntimeError):
            breaker.ejecutar(fallar)
        with self.assertRaises(RuntimeError):
            breaker.ejecutar(fallar)
        with self.assertRaises(CircuitoAbierto):
            breaker.ejecutar(fallar)

        time.sleep(0.06)
        self.assertEqual(breaker.ejecutar(lambda: "ok"), "ok")
        self.assertEqual(breaker.estado, CircuitBreaker.CERRADO)
        self.assertGreaterEqual(breaker.aperturas, 1)
        self.assertGreaterEqual(breaker.rechazos, 1)


if __name__ == "__main__":
    unittest.main()
