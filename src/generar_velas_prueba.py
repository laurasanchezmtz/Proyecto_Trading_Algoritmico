"""
Genera velas FICTICIAS de EURUSD de 1 minuto y las guarda en un CSV.

Para qué sirve:
  - Probar la estrategia y el backtesting sin internet ni IBKR.
  - Tener SIEMPRE los mismos datos (semilla fija) para que todo el equipo
    obtenga los mismos resultados.

Uso:
  python src/generar_velas_prueba.py
  python src/generar_velas_prueba.py --velas 3000 --semilla 7

IMPORTANTE: son datos inventados. Los resultados del backtest con estos datos
NO dicen nada sobre si la estrategia gana dinero en la realidad.
"""

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from estrategia_sma import escribir_fecha


def generar_velas(cantidad=2000, semilla=42, precio_inicial=1.0850, symbol="EURUSD"):
    azar = random.Random(semilla)
    inicio = datetime(2026, 11, 16, 0, 0, tzinfo=timezone.utc)
    precio = precio_inicial
    tendencia = 0.0
    velas = []

    for i in range(cantidad):
        # Cada ~120 velas cambia la "tendencia" del mercado, para que haya subidas y bajadas
        if i % 120 == 0:
            tendencia = azar.uniform(-0.000015, 0.000015)

        num_ticks = azar.randint(5, 40)
        apertura = precio
        maximo = minimo = precio
        for _ in range(num_ticks):
            precio += tendencia / num_ticks + azar.gauss(0, 0.00003)
            maximo = max(maximo, precio)
            minimo = min(minimo, precio)

        velas.append({
            "symbol": symbol,
            "inicio": escribir_fecha(inicio + timedelta(minutes=i)),
            "open": round(apertura, 5),
            "high": round(maximo, 5),
            "low": round(minimo, 5),
            "close": round(precio, 5),
            "num_ticks": num_ticks,
        })
    return velas


def guardar_csv(velas, ruta):
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=list(velas[0].keys()))
        escritor.writeheader()
        escritor.writerows(velas)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Genera velas ficticias de EURUSD.")
    parser.add_argument("--velas", type=int, default=2000)
    parser.add_argument("--semilla", type=int, default=42)
    parser.add_argument("--salida", default="datos_prueba/velas_sinteticas.csv")
    args = parser.parse_args()

    velas = generar_velas(args.velas, args.semilla)
    guardar_csv(velas, args.salida)
    print(f"Se guardaron {len(velas)} velas ficticias en {args.salida}")
