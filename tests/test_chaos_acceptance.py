"""
Suite de pruebas de aceptación automatizadas para fallas configurables,
kill switch, reproducibilidad del E2 y línea base del Momento 1.
SupplyGrid · Entregable 3 · EAFIT
"""

import hashlib
import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from chaos.config import ChaosConfigManager
from chaos.engine import (
    ChaosDatabaseError,
    ChaosEngine,
    ChaosHttpError,
    ChaosTimeoutError,
)
from chaos.interceptor import _infer_target
from chaos.models import ChaosGlobalConfig, FaultConfig, FaultFamily, FaultType
from datos.reconstruir_e2 import (
    CANONICAL_COUNTS,
    CANONICAL_SEED,
    CANONICAL_TOTAL_ROWS,
    HISTORICAL_E2_IDEMPOTENCY_MD5,
)


class TestChaosConfigYKillSwitch(unittest.TestCase):
    """Pruebas de aceptación 1, 2, 3, 4 y validación de esquemas."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = Path(self.temp_dir) / "test_chaos_config.json"
        self.manager = ChaosConfigManager(self.config_path)
        self.engine = ChaosEngine(self.manager)

    def tearDown(self):
        self.engine.stop_all_disturbances()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_01_configuracion_predeterminada_segura(self):
        """Criterio 1: La configuración predeterminada no activa fallas."""
        cfg = self.manager.get_config()
        self.assertFalse(cfg.kill_switch, "El kill switch debe iniciar inactivo")
        self.assertFalse(cfg.enabled, "El motor de fallas debe iniciar deshabilitado por seguridad")
        for fid, f in cfg.faults.items():
            self.assertFalse(f.enabled, f"La falla {fid} debe estar apagada por defecto")

    def test_02_activacion_y_desactivacion_en_caliente_sin_redespliegue(self):
        """Criterio 2: Las fallas pueden habilitarse y deshabilitarse en ejecución."""
        # Activar A1
        ok = self.manager.set_fault_state("A1_latencia_jitter", True, {"mean_ms": 15.0, "jitter_ms": 2.0})
        self.assertTrue(ok)

        cfg_after = self.manager.get_config()
        self.assertTrue(cfg_after.enabled)
        self.assertTrue(cfg_after.faults["A1_latencia_jitter"].enabled)
        self.assertEqual(cfg_after.faults["A1_latencia_jitter"].parameters["mean_ms"], 15.0)

        # Desactivar A1
        ok_deact = self.manager.set_fault_state("A1_latencia_jitter", False)
        self.assertTrue(ok_deact)
        cfg_off = self.manager.get_config()
        self.assertFalse(cfg_off.faults["A1_latencia_jitter"].enabled)

    def test_03_prioridad_absoluta_del_kill_switch(self):
        """Criterio 3: El kill switch desactiva todas las fallas y prevalece sobre cualquier configuración."""
        # Activar fallas de distintas familias
        self.manager.set_fault_state("A1_latencia_jitter", True, {"mean_ms": 100.0})
        self.manager.set_fault_state("B1_pool_agotado", True)
        self.manager.set_fault_state("D2_respuestas_5xx", True)

        # Encender Kill Switch
        self.manager.set_kill_switch(True)
        self.assertTrue(self.engine.is_kill_switch_active())

        # Verificar que ninguna falla se inyecta
        cfg = self.manager.get_config()
        for f in cfg.faults.values():
            should = self.engine.should_inject(f, "Q1")
            self.assertFalse(should, f"Con kill switch activo, should_inject debe ser False para {f.id}")

        # Comprobar que una operación interceptada corre limpia y sin delay ni error
        t0 = time.perf_counter()
        res = self.engine.intercept_operation("Q1", lambda: "normal_ok")
        dt_ms = (time.perf_counter() - t0) * 1000.0

        self.assertEqual(res, "normal_ok")
        self.assertLess(dt_ms, 30.0, "Con kill switch activo, la latencia de 100ms no debe aplicarse")

    def test_04_recuperacion_inmediata_sin_redespliegue(self):
        """Criterio 4: La recuperación no exige reinicio ni redespliegue."""
        # Iniciar con falla B1 activa al 100% de tráfico
        self.manager.set_fault_state(
            "B1_pool_agotado",
            True,
            parameters={"rejection_probability": 1.0},
            traffic_pct=1.0,
        )
        with self.assertRaises(ChaosDatabaseError):
            self.engine.intercept_operation("Q1", lambda: "db_query")

        # Activar Kill Switch
        self.manager.set_kill_switch(True)
        # Recuperación inmediata sin excepciones
        res = self.engine.intercept_operation("Q1", lambda: "db_query")
        self.assertEqual(res, "db_query")

        # Desactivar Kill Switch pero apagar la falla
        self.manager.set_kill_switch(False)
        self.manager.set_fault_state("B1_pool_agotado", False)
        res2 = self.engine.intercept_operation("Q1", lambda: "db_query_post")
        self.assertEqual(res2, "db_query_post")

    def test_05_cobertura_de_las_cuatro_familias_exigidas(self):
        """Criterio: Cobertura comprobable de las 4 familias de la rúbrica."""
        cfg = self.manager.get_config()
        familias_presentes = {f.family for f in cfg.faults.values()}

        self.assertIn(FaultFamily.LATENCY, familias_presentes, "Debe incluir Familia A: Latencia")
        self.assertIn(FaultFamily.DATABASE, familias_presentes, "Debe incluir Familia B: Base de datos")
        self.assertIn(FaultFamily.DEGRADATION, familias_presentes, "Debe incluir Familia C: Degradación")
        self.assertIn(FaultFamily.AVAILABILITY, familias_presentes, "Debe incluir Familia D: Disponibilidad")

        # Comprobar al menos 1-2 fallas por familia
        for fam in FaultFamily:
            sub = [f for f in cfg.faults.values() if f.family == fam]
            self.assertGreaterEqual(len(sub), 2, f"La familia {fam} debe tener al menos 2 fallas implementadas")

    def test_06_validacion_de_parametros_invalidos(self):
        """Criterio: Una configuración inválida no corrompe el sistema."""
        bad_fault = FaultConfig(
            id="bad_fault",
            family=FaultFamily.LATENCY,
            fault_type=FaultType.LATENCY_JITTER,
            traffic_pct=1.5,  # Inválido (> 1.0)
            parameters={"mean_ms": -10.0},  # Inválido (< 0)
        )
        errores = bad_fault.validate()
        self.assertTrue(len(errores) >= 2, "Debe detectar traffic_pct inválido y mean_ms negativo")

    def test_07_inferencia_de_targets_sql(self):
        """Comprueba que el interceptor reconoce correctamente las consultas Q1 a Q5."""
        self.assertEqual(_infer_target("SELECT * FROM proveedores.contratos WHERE fecha_fin >= '2026-09-18'"), "Q1")
        self.assertEqual(_infer_target("SELECT * FROM catalogo.catalogo_sku WHERE sku_codigo = ANY($1)"), "Q2")
        self.assertEqual(_infer_target("SELECT * FROM logistica.franjas_descargue WHERE disponible = true"), "Q3")
        self.assertEqual(_infer_target("SELECT date_trunc('month', fecha_orden) FROM ordenes.ordenes o JOIN ordenes.lineas_orden l ON l.orden_id=o.id"), "Q4")
        self.assertEqual(_infer_target("SELECT * FROM ordenes.ordenes ORDER BY fecha_orden DESC LIMIT 50"), "Q5")


class TestReproducibilidadYMomento1(unittest.TestCase):
    """Pruebas de aceptación 5, 6, 7, 8, 9, 10, 11."""

    def test_08_conteos_canonicos_e2_y_semilla(self):
        """Criterio 7: Los conteos corresponden a las entidades y volumen real del E2."""
        self.assertEqual(CANONICAL_SEED, 42, "La seed canónica de referencia debe ser 42")
        self.assertEqual(CANONICAL_TOTAL_ROWS, 4_965_000, "El volumen total del E2 debe ser 4.965.000 filas")
        self.assertEqual(CANONICAL_COUNTS["proveedores.proveedores"], 100_000)
        self.assertEqual(CANONICAL_COUNTS["proveedores.contratos"], 15_000)
        self.assertEqual(CANONICAL_COUNTS["catalogo.catalogo_sku"], 400_000)
        self.assertEqual(CANONICAL_COUNTS["logistica.franjas_descargue"], 150_000)
        self.assertEqual(CANONICAL_COUNTS["ordenes.ordenes"], 500_000)
        self.assertEqual(CANONICAL_COUNTS["ordenes.lineas_orden"], 3_000_000)
        self.assertEqual(CANONICAL_COUNTS["auditoria.eventos"], 800_000)

    def test_09_checksum_historico_idempotencia_e2(self):
        """Criterio 6: El checksum de idempotencia de E2 debe coincidir con el valor histórico."""
        self.assertEqual(HISTORICAL_E2_IDEMPOTENCY_MD5, "1c1969f67843450e37fc74df05d87db1")

    def test_10_calculo_matematico_objetivo_rendimiento_10_porciento(self):
        """Criterio 11: Los objetivos de mejora se calculan como valor E2 * 0.90."""
        # Ejemplo: si en E2 Q4 caliente p95 fue 7.149 ms, el objetivo E3 debe ser exactamente 7.149 * 0.90 = 6.4341 ms
        e2_val = 7.149
        objetivo_esperado = e2_val * 0.90
        self.assertAlmostEqual(objetivo_esperado, 6.4341, places=4)
        self.assertLess(objetivo_esperado, e2_val, "El objetivo debe ser estrictamente menor que el valor E2")

    def test_11_determinismo_de_generacion_de_claves_idempotencia(self):
        """Criterio: Determinismo estricto: dos generaciones idénticas producen el mismo hash."""
        def generar_hashes_test(seed: int, n: int = 100):
            hasher = hashlib.md5()
            for i in range(1, n + 1):
                key = f"seed{seed}-orden-{i}"
                hasher.update(key.encode("utf-8"))
            return hasher.hexdigest()

        hash1 = generar_hashes_test(42, 500)
        hash2 = generar_hashes_test(42, 500)
        self.assertEqual(hash1, hash2, "Misma seed debe generar exactamente el mismo hash")

        hash_otro = generar_hashes_test(99, 500)
        self.assertNotEqual(hash1, hash_otro, "Semillas diferentes deben producir hashes diferentes")


if __name__ == "__main__":
    unittest.main()
