"""
Descarga velas HISTÓRICAS reales de EURUSD (Yahoo Finance) y las guarda con el
formato oficial de `velas` para usarlas en el backtesting.

Uso (necesita internet):
  python -m pip install yfinance
  python src/descargar_datos.py                       # 1 minuto, últimos 7 días
  python src/descargar_datos.py --intervalo 1h --periodo 1y

Límites de Yahoo: las velas de 1 minuto solo existen para los últimos ~7 días;
las de 1 hora, hasta ~2 años. Yahoo no publica bid/ask ni el número de ticks,
así que `num_ticks` queda en 0. Es una fuente gratuita para la escuela, no una
fuente profesional; IBKR también ofrece datos históricos cuando la cuenta esté lista.
"""

import argparse
from pathlib import Path

import yfinance as yf

TICKERS = {"EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X"}


def descargar(symbol="EURUSD", periodo="7d", intervalo="1m"):
    datos = yf.download(TICKERS[symbol], period=periodo, interval=intervalo,
                        progress=False, auto_adjust=False)
    if datos.empty:
        raise SystemExit("No se descargaron datos. Revisa tu conexión o los límites de periodo/intervalo.")

    # Las versiones recientes de yfinance devuelven columnas de dos niveles: las aplanamos
    if hasattr(datos.columns, "levels") and datos.columns.nlevels > 1:
        datos.columns = datos.columns.get_level_values(0)

    datos.index = datos.index.tz_convert("UTC") if datos.index.tz else datos.index.tz_localize("UTC")
    velas = datos.reset_index()
    columna_fecha = velas.columns[0]

    resultado = velas.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close"})
    resultado["inicio"] = velas[columna_fecha].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    resultado["symbol"] = symbol
    resultado["num_ticks"] = 0
    return resultado[["symbol", "inicio", "open", "high", "low", "close", "num_ticks"]].dropna()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descarga velas históricas de FOREX.")
    parser.add_argument("--symbol", default="EURUSD", choices=TICKERS.keys())
    parser.add_argument("--periodo", default="7d")
    parser.add_argument("--intervalo", default="1m")
    args = parser.parse_args()

    velas = descargar(args.symbol, args.periodo, args.intervalo)
    ruta = Path(f"datos_prueba/{args.symbol}_{args.intervalo}_{args.periodo}.csv")
    ruta.parent.mkdir(parents=True, exist_ok=True)
    velas.to_csv(ruta, index=False)
    print(f"Se guardaron {len(velas)} velas reales en {ruta}")
