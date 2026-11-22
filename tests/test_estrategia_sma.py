"""
Pruebas automáticas de la estrategia SMA. Cada prueba arma velas "a mano"
con un resultado conocido y verifica que la estrategia decida lo esperado.

Ejecutar desde la raíz del proyecto:
  python -m unittest discover tests -v
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from estrategia_sma import EstrategiaSMA, escribir_fecha  # noqa: E402

INICIO = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)


def vela(minuto, close, symbol="EURUSD"):
    return {
        "symbol": symbol,
        "inicio": escribir_fecha(INICIO + timedelta(minutes=minuto)),
        "open": close, "high": close, "low": close, "close": close, "num_ticks": 10,
    }


def alimentar(estrategia, precios, desde=0):
    """Pasa una lista de precios como velas consecutivas y devuelve las señales emitidas."""
    senales = []
    for i, precio in enumerate(precios):
        senal = estrategia.procesar_vela(vela(desde + i, precio))
        if senal:
            senal["_motivo"] = estrategia.ultimo_motivo  # solo para las pruebas
            senales.append(senal)
    return senales


class PruebasEstrategiaSMA(unittest.TestCase):

    def test_no_hay_senal_antes_de_50_velas(self):
        estrategia = EstrategiaSMA()
        senales = alimentar(estrategia, [1.08 + i * 0.0001 for i in range(49)])
        self.assertEqual(senales, [])

    def test_compra_cuando_el_precio_sube(self):
        estrategia = EstrategiaSMA()
        senales = alimentar(estrategia, [1.08 + i * 0.0001 for i in range(50)])
        self.assertEqual(len(senales), 1)
        self.assertEqual(senales[0]["signal"], "BUY")

    def test_no_repite_compra_si_ya_esta_comprada(self):
        estrategia = EstrategiaSMA()
        senales = alimentar(estrategia, [1.08 + i * 0.0001 for i in range(80)])
        self.assertEqual([s["signal"] for s in senales], ["BUY"])

    def test_formato_oficial_de_la_senal(self):
        estrategia = EstrategiaSMA()
        senal = alimentar(estrategia, [1.08 + i * 0.0001 for i in range(50)])[0]
        senal.pop("_motivo")
        self.assertEqual(set(senal), {"id_algoritmo", "symbol", "signal", "precio_referencia", "timestamp"})
        self.assertEqual(senal["symbol"], "EURUSD")
        # La vela 49 empieza 12:49 y termina 12:50: la señal se fecha al cierre
        self.assertEqual(senal["timestamp"], "2026-11-21T12:50:00Z")

    def test_vende_por_stop_loss(self):
        estrategia = EstrategiaSMA()
        alimentar(estrategia, [1.08 + i * 0.0001 for i in range(50)])  # compra a 1.0849
        precio_caida = 1.0849 * 0.97                                     # cae 3 %
        senal = estrategia.procesar_vela(vela(50, precio_caida))
        self.assertEqual(senal["signal"], "SELL")
        self.assertEqual(estrategia.ultimo_motivo, "STOP_LOSS")

    def test_vende_por_take_profit(self):
        estrategia = EstrategiaSMA()
        alimentar(estrategia, [1.08 + i * 0.0001 for i in range(50)])
        senal = estrategia.procesar_vela(vela(50, 1.0849 * 1.05))       # sube 5 %
        self.assertEqual(senal["signal"], "SELL")
        self.assertEqual(estrategia.ultimo_motivo, "TAKE_PROFIT")

    def test_vende_por_cruce_bajista(self):
        estrategia = EstrategiaSMA()
        subida = [1.08 + i * 0.0001 for i in range(50)]
        bajada = [subida[-1] - i * 0.0002 for i in range(1, 40)]        # baja poco a poco (< 2 %)
        senales = alimentar(estrategia, subida + bajada)
        self.assertEqual([s["signal"] for s in senales], ["BUY", "SELL"])
        self.assertEqual(senales[1]["_motivo"], "CRUCE_BAJISTA")

    def test_ignora_vela_repetida(self):
        estrategia = EstrategiaSMA()
        alimentar(estrategia, [1.08] * 10)
        estrategia.procesar_vela(vela(5, 1.09))  # minuto 5 otra vez: debe ignorarse
        self.assertEqual(len(estrategia.cierres), 10)

    def test_ignora_otro_simbolo(self):
        estrategia = EstrategiaSMA()
        estrategia.procesar_vela(vela(0, 1.27, symbol="GBPUSD"))
        self.assertEqual(len(estrategia.cierres), 0)

    def test_solo_en_cruce_no_compra_si_ya_venia_arriba(self):
        # Si desde la primera SMA válida la corta ya está arriba, no hubo cruce: no compra
        estrategia = EstrategiaSMA(solo_en_cruce=True)
        senales = alimentar(estrategia, [1.08 + i * 0.0001 for i in range(80)])
        self.assertEqual(senales, [])


if __name__ == "__main__":
    unittest.main()
