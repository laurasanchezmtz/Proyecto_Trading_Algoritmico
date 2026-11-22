"""
Backtesting de la estrategia SMA 10/50.

Idea: "hacer como si" las velas de un periodo pasado llegaran una por una,
pasárselas a la estrategia (la MISMA clase que se usa en vivo) y registrar
qué habría pasado con cada operación.

Uso:
  python src/backtesting.py                                  # datos ficticios
  python src/backtesting.py --datos datos_prueba/EURUSD_1m_7d.csv
  python src/backtesting.py --solo-cruce                     # variante que espera el cruce

Genera en la carpeta resultados/:
  - operaciones.csv  -> cada operación (entrada, salida, motivo, pips, USD)
  - resumen.txt      -> métricas principales
  - grafica.png      -> precio, medias móviles, compras/ventas y ganancia acumulada

Supuestos de la simulación (hay que mencionarlos en la exposición):
  - Solo se abren posiciones de COMPRA (así lo plantea el pseudocódigo).
  - Se opera al precio de cierre de la vela que generó la señal.
  - El spread se cobra así: se compra medio spread más caro y se vende medio spread más barato.
  - No se cobran comisiones ni swaps (costos por mantener la posición de un día a otro).
  - Capital fijo por operación (1000 USD por defecto, como en el pseudocódigo).
"""

import argparse
from pathlib import Path

import pandas as pd

from estrategia_sma import EstrategiaSMA, leer_fecha
from generar_velas_prueba import generar_velas

PIP = 0.0001  # en EURUSD y GBPUSD, 1 pip = 0.0001


def cargar_velas(ruta):
    if ruta is None:
        print("No se indicó --datos: se usan 2000 velas FICTICIAS (semilla 42).")
        return generar_velas(2000, 42)
    tabla = pd.read_csv(ruta)
    return tabla.to_dict("records")


def ejecutar_backtest(velas, estrategia, spread=0.0001, capital=1000.0):
    """Recorre las velas en orden y devuelve (operaciones, registro_por_vela)."""
    operaciones = []
    registro = []          # una fila por vela, para la gráfica
    entrada = None

    for vela in velas:
        senal = estrategia.procesar_vela(vela)
        fila = {
            "momento": leer_fecha(vela["inicio"]),
            "close": float(vela["close"]),
            "sma_corta": estrategia.ultima_sma_corta if len(estrategia.cierres) >= estrategia.periodo_largo else None,
            "sma_larga": estrategia.ultima_sma_larga if len(estrategia.cierres) >= estrategia.periodo_largo else None,
            "signal": senal["signal"] if senal else None,
        }
        registro.append(fila)

        if senal is None:
            continue
        precio = senal["precio_referencia"]

        if senal["signal"] == "BUY":
            entrada = {"entrada_hora": senal["timestamp"], "entrada_precio": precio + spread / 2}
        elif senal["signal"] == "SELL" and entrada is not None:
            operaciones.append(_cerrar(entrada, senal["timestamp"], precio - spread / 2,
                                       estrategia.ultimo_motivo, capital))
            entrada = None

    # Si al final de los datos seguimos comprados, cerramos al último precio
    if entrada is not None:
        ultima = velas[-1]
        operaciones.append(_cerrar(entrada, ultima["inicio"], float(ultima["close"]) - spread / 2,
                                   "FIN_DE_DATOS", capital))

    return pd.DataFrame(operaciones), pd.DataFrame(registro)


def _cerrar(entrada, hora_salida, precio_salida, motivo, capital):
    unidades = capital / entrada["entrada_precio"]  # cuántos euros compramos con el capital
    diferencia = precio_salida - entrada["entrada_precio"]
    return {
        **entrada,
        "salida_hora": hora_salida,
        "salida_precio": round(precio_salida, 5),
        "motivo_salida": motivo,
        "pips": round(diferencia / PIP, 1),
        "ganancia_usd": round(unidades * diferencia, 4),
    }


def calcular_metricas(operaciones, velas, spread):
    if operaciones.empty:
        return {"operaciones": 0}
    ganadoras = operaciones[operaciones["pips"] > 0]
    acumulado = operaciones["pips"].cumsum()
    caida_maxima = (acumulado.cummax().clip(lower=0) - acumulado).max()
    comprar_y_mantener = (float(velas[-1]["close"]) - float(velas[0]["close"]) - spread) / PIP

    return {
        "velas analizadas": len(velas),
        "periodo": f'{velas[0]["inicio"]} a {velas[-1]["inicio"]}',
        "operaciones": len(operaciones),
        "ganadoras": len(ganadoras),
        "% ganadoras": round(100 * len(ganadoras) / len(operaciones), 1),
        "pips totales": round(operaciones["pips"].sum(), 1),
        "pips promedio por operación": round(operaciones["pips"].mean(), 2),
        "mejor operación (pips)": operaciones["pips"].max(),
        "peor operación (pips)": operaciones["pips"].min(),
        "caída máxima (pips)": round(caida_maxima, 1),
        "ganancia total (USD)": round(operaciones["ganancia_usd"].sum(), 2),
        "costo total por spread (pips)": round(len(operaciones) * spread / PIP, 1),
        "salidas por motivo": operaciones["motivo_salida"].value_counts().to_dict(),
        "comparación: comprar y mantener (pips)": round(comprar_y_mantener, 1),
    }


def graficar(registro, operaciones, ruta, titulo):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    texto, texto2, fondo, rejilla = "#0b0b0b", "#52514e", "#fcfcfb", "#e4e3df"
    azul, naranja, aqua, rojo = "#2a78d6", "#eb6834", "#1baf7a", "#e34948"

    fig, (arriba, abajo) = plt.subplots(2, 1, figsize=(13, 8), sharex=True,
                                        gridspec_kw={"height_ratios": [3, 1.3]})
    fig.patch.set_facecolor(fondo)
    for eje in (arriba, abajo):
        eje.set_facecolor(fondo)
        eje.grid(color=rejilla, linewidth=0.8)
        for lado in ("top", "right"):
            eje.spines[lado].set_visible(False)
        for lado in ("left", "bottom"):
            eje.spines[lado].set_color(rejilla)
        eje.tick_params(colors=texto2, labelsize=9)

    arriba.plot(registro["momento"], registro["close"], color="#a9a8a2", linewidth=1, label="Precio de cierre")
    arriba.plot(registro["momento"], registro["sma_corta"], color=azul, linewidth=2, label="SMA corta")
    arriba.plot(registro["momento"], registro["sma_larga"], color=naranja, linewidth=2, label="SMA larga")

    compras = registro[registro["signal"] == "BUY"]
    ventas = registro[registro["signal"] == "SELL"]
    arriba.scatter(compras["momento"], compras["close"], marker="^", s=70, color=aqua,
                   edgecolors=fondo, linewidths=1.5, zorder=3, label="BUY")
    arriba.scatter(ventas["momento"], ventas["close"], marker="v", s=70, color=rojo,
                   edgecolors=fondo, linewidths=1.5, zorder=3, label="SELL")
    arriba.set_ylabel("EURUSD", color=texto2)
    arriba.set_title(titulo, color=texto, fontsize=13, loc="left")
    arriba.legend(frameon=False, fontsize=9, labelcolor=texto2, ncol=5, loc="upper left")

    if not operaciones.empty:
        horas = pd.to_datetime(operaciones["salida_hora"], utc=True)
        abajo.step(horas, operaciones["pips"].cumsum(), where="post", color=azul, linewidth=2)
        abajo.axhline(0, color=texto2, linewidth=0.8)
    abajo.set_ylabel("Pips acumulados", color=texto2)
    abajo.set_xlabel("Tiempo (UTC)", color=texto2)

    fig.tight_layout()
    fig.savefig(ruta, dpi=130, facecolor=fondo)
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtesting de la estrategia SMA.")
    parser.add_argument("--datos", help="CSV con velas (formato oficial). Si se omite, usa datos ficticios.")
    parser.add_argument("--corta", type=int, default=10)
    parser.add_argument("--larga", type=int, default=50)
    parser.add_argument("--stop-loss", type=float, default=0.02, help="0.02 = 2 %% de variación del precio")
    parser.add_argument("--take-profit", type=float, default=0.04)
    parser.add_argument("--spread", type=float, default=0.0001, help="0.0001 = 1 pip")
    parser.add_argument("--capital", type=float, default=1000.0)
    parser.add_argument("--solo-cruce", action="store_true", help="Comprar solo en el cruce alcista")
    parser.add_argument("--salida", default="resultados")
    args = parser.parse_args()

    velas = cargar_velas(args.datos)
    if len(velas) < 2:
        raise SystemExit("Se necesitan al menos 2 velas.")
    duracion = leer_fecha(velas[1]["inicio"]) - leer_fecha(velas[0]["inicio"])  # 1 min, 1 h...

    estrategia = EstrategiaSMA(
        id_algoritmo=f"SMA_{args.corta}_{args.larga}",
        symbol=velas[0]["symbol"],
        periodo_corto=args.corta,
        periodo_largo=args.larga,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
        solo_en_cruce=args.solo_cruce,
        duracion_vela=duracion,
    )

    operaciones, registro = ejecutar_backtest(velas, estrategia, args.spread, args.capital)
    metricas = calcular_metricas(operaciones, velas, args.spread)

    carpeta = Path(args.salida)
    carpeta.mkdir(parents=True, exist_ok=True)
    operaciones.to_csv(carpeta / "operaciones.csv", index=False)

    variante = "solo en cruce" if args.solo_cruce else "pseudocódigo del profesor"
    lineas = [f"Backtesting {estrategia.id_algoritmo} ({variante}) sobre {estrategia.symbol}"]
    lineas += [f"  {clave}: {valor}" for clave, valor in metricas.items()]
    resumen = "\n".join(lineas)
    (carpeta / "resumen.txt").write_text(resumen, encoding="utf-8")
    print(resumen)

    graficar(registro, operaciones, carpeta / "grafica.png",
             f"{estrategia.id_algoritmo} · {estrategia.symbol} · {variante}")
    print(f"\nArchivos guardados en {carpeta}/")
