"""
Estrategia de cruce de medias móviles simples (SMA 10 / SMA 50) para FOREX.

Basada en el pseudocódigo "Trading_Cruce_Medias_Moviles" del profesor,
adaptada de BTC/USDT a EURUSD.

Este archivo contiene SOLO la lógica de decisión. No sabe nada de Kafka,
de Spark ni de Interactive Brokers. Recibe una vela y responde:
  - None            -> no hacer nada
  - un diccionario  -> una señal con el formato oficial del tópico `signals`

Gracias a esa separación, la MISMA clase se usa en dos lugares:
  1. estrategia_kafka.py -> en vivo (lee `velas`, publica en `signals`)
  2. backtesting.py      -> con datos históricos guardados en un CSV
"""

from collections import deque
from datetime import datetime, timedelta, timezone


def leer_fecha(texto):
    """Convierte '2026-11-21T17:51:00Z' (u otras variantes ISO 8601) a datetime en UTC."""
    fecha = datetime.fromisoformat(str(texto).replace("Z", "+00:00"))
    if fecha.tzinfo is None:  # si no trae zona horaria, asumimos UTC
        fecha = fecha.replace(tzinfo=timezone.utc)
    return fecha.astimezone(timezone.utc)


def escribir_fecha(fecha):
    """Convierte un datetime a texto ISO 8601 en UTC, por ejemplo '2026-11-21T17:52:00Z'."""
    return fecha.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class EstrategiaSMA:
    """
    Reglas (pseudocódigo del profesor):
      - COMPRAR (BUY) si SMA corta > SMA larga y no hay posición abierta.
      - CERRAR (SELL) si hay posición abierta y ocurre cualquiera de estas:
          * Stop Loss:   el precio cayó stop_loss_pct desde la entrada (2 %)
          * Take Profit: el precio subió take_profit_pct desde la entrada (4 %)
          * Cruce bajista: SMA corta < SMA larga

    Opción extra `solo_en_cruce` (no viene en el pseudocódigo, por defecto apagada):
      Si es True, solo compra en la vela donde la SMA corta CRUZA hacia arriba a la
      larga. Así, después de un Stop Loss no vuelve a comprar de inmediato si las
      medias siguen igual. Sirve para comparar ambas versiones en el backtesting.
    """

    def __init__(
        self,
        id_algoritmo="SMA_10_50",
        symbol="EURUSD",
        periodo_corto=10,
        periodo_largo=50,
        stop_loss_pct=0.02,
        take_profit_pct=0.04,
        solo_en_cruce=False,
        duracion_vela=timedelta(minutes=1),
    ):
        if periodo_corto >= periodo_largo:
            raise ValueError("El periodo corto debe ser menor que el largo.")

        self.id_algoritmo = id_algoritmo
        self.symbol = symbol
        self.periodo_corto = periodo_corto
        self.periodo_largo = periodo_largo
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.solo_en_cruce = solo_en_cruce
        self.duracion_vela = duracion_vela

        # Memoria de la estrategia (su "estado")
        self.cierres = deque(maxlen=periodo_largo)  # guarda solo los últimos 50 cierres
        self.en_posicion = False
        self.precio_entrada = None
        self.ultimo_inicio = None          # para ignorar velas repetidas o atrasadas
        self.diferencia_anterior = None    # SMA corta - SMA larga en la vela anterior

        # Información para explicar la última decisión (no se publica en Kafka)
        self.ultimo_motivo = None
        self.ultima_sma_corta = None
        self.ultima_sma_larga = None

    # ------------------------------------------------------------------
    def calcular_sma(self, n):
        """Promedio de los últimos n precios de cierre."""
        ultimos = list(self.cierres)[-n:]
        return sum(ultimos) / n

    # ------------------------------------------------------------------
    def procesar_vela(self, vela):
        """
        Recibe una vela con el formato oficial:
          {symbol, inicio, open, high, low, close, num_ticks}
        Devuelve None o una señal:
          {id_algoritmo, symbol, signal, precio_referencia, timestamp}
        """
        self.ultimo_motivo = None

        # 1) Solo nos interesa nuestro par de divisas
        if vela.get("symbol") != self.symbol:
            return None

        # 2) Ignorar velas repetidas o que llegan tarde (Kafka puede reenviar mensajes)
        inicio = leer_fecha(vela["inicio"])
        if self.ultimo_inicio is not None and inicio <= self.ultimo_inicio:
            return None
        self.ultimo_inicio = inicio

        # 3) Guardar el precio de cierre
        cierre = float(vela["close"])
        self.cierres.append(cierre)

        # 4) Sin 50 velas no se puede calcular la SMA larga: esperar
        if len(self.cierres) < self.periodo_largo:
            return None

        sma_corta = self.calcular_sma(self.periodo_corto)
        sma_larga = self.calcular_sma(self.periodo_largo)
        self.ultima_sma_corta, self.ultima_sma_larga = sma_corta, sma_larga

        diferencia = sma_corta - sma_larga
        hubo_cruce_alcista = (
            self.diferencia_anterior is not None
            and self.diferencia_anterior <= 0
            and diferencia > 0
        )
        self.diferencia_anterior = diferencia

        # La señal se fecha al CIERRE de la vela, que es cuando ya conocemos su precio final
        momento = inicio + self.duracion_vela

        # 5a) Sin posición: ¿compramos?
        if not self.en_posicion:
            condicion_compra = hubo_cruce_alcista if self.solo_en_cruce else diferencia > 0
            if condicion_compra:
                self.en_posicion = True
                self.precio_entrada = cierre
                self.ultimo_motivo = "SMA_CORTA_MAYOR"
                return self._crear_senal("BUY", cierre, momento)
            return None

        # 5b) Con posición: ¿cerramos?
        variacion = (cierre - self.precio_entrada) / self.precio_entrada
        if variacion <= -self.stop_loss_pct:
            motivo = "STOP_LOSS"
        elif variacion >= self.take_profit_pct:
            motivo = "TAKE_PROFIT"
        elif diferencia < 0:
            motivo = "CRUCE_BAJISTA"
        else:
            return None

        self.en_posicion = False
        self.precio_entrada = None
        self.ultimo_motivo = motivo
        return self._crear_senal("SELL", cierre, momento)

    # ------------------------------------------------------------------
    def _crear_senal(self, tipo, precio, momento):
        """Arma la señal EXACTAMENTE con los campos acordados entre equipos."""
        return {
            "id_algoritmo": self.id_algoritmo,
            "symbol": self.symbol,
            "signal": tipo,
            "precio_referencia": round(precio, 5),
            "timestamp": escribir_fecha(momento),
        }
