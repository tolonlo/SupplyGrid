#!/usr/bin/env python3
"""
Lanzador directo del Tablero en Tiempo Real de SupplyGrid.

Uso:
    python tablero.py
    python tablero.py 8080
"""
import sys
from chaos.tablero import iniciar_tablero, PUERTO_DEFECTO

if __name__ == "__main__":
    puerto = int(sys.argv[1]) if len(sys.argv) > 1 else PUERTO_DEFECTO
    iniciar_tablero(puerto=puerto)
