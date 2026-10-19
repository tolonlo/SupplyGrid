"""
CLI de administración dinámica de fallas para SupplyGrid.
Permite activar, desactivar, inspeccionar y accionar el Kill Switch en caliente,
cumpliendo con la condición obligatoria de no requerir redespliegue.
"""

import argparse
import json
import sys
import time

from .config import ChaosConfigManager
from .engine import ChaosEngine, ChaosException
from .models import FaultFamily
from . import observabilidad as obs


def _cmd_status(manager: ChaosConfigManager):
    cfg = manager.get_config()
    print("=" * 70)
    print(" ESTADO DEL LABORATORIO DE FALLAS CONFIGURABLES (SUPPLYGRID) ")
    print("=" * 70)
    print(f"  Kill Switch Global : {'[ACTIVADO - TODAS LAS FALLAS ANULADAS]' if cfg.kill_switch else '[INACTIVO]'}")
    print(f"  Motor de Fallas    : {'[HABILITADO]' if cfg.enabled else '[DESHABILITADO]'}")
    print(f"  Archivo de Config  : {manager.config_path.resolve()}")
    print("-" * 70)

    # Agrupar por familia
    familias = {
        FaultFamily.LATENCY: "FAMILIA A: Latencia Inducida",
        FaultFamily.DATABASE: "FAMILIA B: Falla de Base de Datos",
        FaultFamily.DEGRADATION: "FAMILIA C: Degradación de Tiempos",
        FaultFamily.AVAILABILITY: "FAMILIA D: Riesgo de Disponibilidad",
    }

    for fam, titulo in familias.items():
        print(f"\n{titulo}")
        sub_faults = [f for f in cfg.faults.values() if f.family == fam]
        for f in sub_faults:
            estado = "ON " if f.enabled else "OFF"
            print(f"  [{estado}] {f.id:<25} | Tráfico: {f.traffic_pct*100:>3.0f}% | Targets: {','.join(f.targets)}")
            print(f"        Descripción: {f.description}")
            print(f"        Parámetros : {json.dumps(f.parameters)}")
    print("=" * 70)


def _cmd_activate(manager: ChaosConfigManager, fault_id: str, raw_params: list):
    params = {}
    for p in raw_params or []:
        if "=" in p:
            k, v = p.split("=", 1)
            # Intento de parseo numérico / bool
            if v.lower() == "true":
                v = True
            elif v.lower() == "false":
                v = False
            else:
                try:
                    v = float(v) if "." in v else int(v)
                except ValueError:
                    pass
            params[k] = v

    ok = manager.set_fault_state(fault_id, True, parameters=params if params else None)
    if ok:
        print(f"[OK] Falla '{fault_id}' ACTIVADA dinámicamente.")
        if params:
            print(f"     Parámetros aplicados: {params}")
    else:
        print(f"[ERROR] No se encontró la falla con id '{fault_id}'.")
        sys.exit(1)


def _cmd_deactivate(manager: ChaosConfigManager, fault_id: str):
    ok = manager.set_fault_state(fault_id, False)
    if ok:
        print(f"[OK] Falla '{fault_id}' DESACTIVADA dinámicamente.")
    else:
        print(f"[ERROR] No se encontró la falla con id '{fault_id}'.")
        sys.exit(1)


def _cmd_kill(manager: ChaosConfigManager):
    manager.set_kill_switch(True)
    engine = ChaosEngine(manager)
    obs.iniciar()
    obs.kill_switch("cli_kill")
    engine.stop_all_disturbances()
    print("=" * 70)
    print(" [KILL SWITCH ACTIVADO GLOBALMENTE] ")
    print(" Todas las fallas han sido ANULADAS de manera inmediata.")
    print(" Ninguna perturbación se inyectará. El sistema opera en modo normal.")
    print("=" * 70)


def _cmd_unkill(manager: ChaosConfigManager):
    manager.set_kill_switch(False)
    print("[OK] Kill Switch DESACTIVADO. La configuración individual vuelve a tener efecto.")


def _cmd_reset(manager: ChaosConfigManager):
    manager.reset_defaults()
    engine = ChaosEngine(manager)
    engine.stop_all_disturbances()
    print("[OK] Catálogo de fallas restablecido a su estado predeterminado (todas OFF, Kill Switch OFF).")


def _cmd_test_run(manager: ChaosConfigManager, target: str, repetitions: int):
    engine = ChaosEngine(manager)
    cfg = manager.get_config()
    print(f"\nProbando inyección de fallas sobre target '{target}' ({repetitions} ejecuciones)...")
    print(f"Estado Kill Switch: {cfg.kill_switch} | Motor habilitado: {cfg.enabled}")

    fallas_activas = [f.id for f in cfg.faults.values() if f.enabled]
    print(f"Fallas activadas individualmente: {fallas_activas if fallas_activas else 'Ninguna'}")

    tiempos = []
    errores = 0

    for i in range(1, repetitions + 1):
        t0 = time.perf_counter()
        try:
            def dummy_op():
                time.sleep(0.001)
                return "ok"
            engine.intercept_operation(target, dummy_op)
            t_ms = (time.perf_counter() - t0) * 1000.0
            tiempos.append(t_ms)
        except ChaosException as ce:
            t_ms = (time.perf_counter() - t0) * 1000.0
            errores += 1
            print(f"  Ejecución #{i:02d}: [INTERCEPTADO] {ce} ({t_ms:.2f} ms)")

    if tiempos:
        avg_ms = sum(tiempos) / len(tiempos)
        print(f"\nResultado test: {len(tiempos)} exitosas (promedio {avg_ms:.2f} ms), {errores} excepciones inyectadas.")


def main():
    parser = argparse.ArgumentParser(description="CLI de Control Dinámico de Fallas · SupplyGrid")
    subparsers = parser.add_subparsers(dest="subcommand", help="Comandos disponibles")

    # status
    subparsers.add_parser("status", help="Muestra el estado actual del laboratorio y catálogo de fallas")

    # activate
    p_act = subparsers.add_parser("activate", help="Activa una falla en caliente")
    p_act.add_argument("fault_id", help="ID de la falla (ej. A1_latencia_jitter)")
    p_act.add_argument("--params", "-p", nargs="*", help="Parámetros clave=valor (ej. mean_ms=50)")

    # deactivate
    p_deact = subparsers.add_parser("deactivate", help="Desactiva una falla en caliente")
    p_deact.add_argument("fault_id", help="ID de la falla")

    # kill
    subparsers.add_parser("kill", help="Activa de inmediato el KILL SWITCH global (prioridad absoluta)")

    # unkill
    subparsers.add_parser("unkill", help="Desactiva el Kill Switch global")

    # reset
    subparsers.add_parser("reset", help="Restablece la configuración a estado seguro (todo apagado)")

    # test-run
    p_test = subparsers.add_parser("test-run", help="Ejecuta una prueba sintética para observar el efecto")
    p_test.add_argument("--target", default="Q1", help="Target a simular (ej. Q1, Q4, POST /ordenes-compra)")
    p_test.add_argument("--repetitions", "-n", type=int, default=10, help="Número de iteraciones")

    args = parser.parse_args()
    manager = ChaosConfigManager()

    if args.subcommand == "status" or not args.subcommand:
        _cmd_status(manager)
    elif args.subcommand == "activate":
        _cmd_activate(manager, args.fault_id, args.params)
    elif args.subcommand == "deactivate":
        _cmd_deactivate(manager, args.fault_id)
    elif args.subcommand == "kill":
        _cmd_kill(manager)
    elif args.subcommand == "unkill":
        _cmd_unkill(manager)
    elif args.subcommand == "reset":
        _cmd_reset(manager)
    elif args.subcommand == "test-run":
        _cmd_test_run(manager, args.target, args.repetitions)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
