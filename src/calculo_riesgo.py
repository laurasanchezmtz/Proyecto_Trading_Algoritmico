"""Calculo de riesgo para posiciones BUY en EURUSD."""

import math

from config_riesgo import (
    CAPITAL_POR_OPERACION_USD,
    RIESGO_MAXIMO_TEORICO_USD,
    STOP_LOSS,
    TAKE_PROFIT,
)


def calcular_riesgo(precio):
    precio = float(precio)

    if not math.isfinite(precio) or precio <= 0:
        raise ValueError("Precio invalido")

    cantidad = math.floor(CAPITAL_POR_OPERACION_USD / precio)

    if cantidad <= 0:
        raise ValueError("Cantidad insuficiente")

    exposicion = cantidad * precio
    riesgo = exposicion * STOP_LOSS
    beneficio = exposicion * TAKE_PROFIT

    return {
        "cantidad_eur": cantidad,
        "exposicion_usd": exposicion,
        "precio_entrada": precio,
        "stop_loss": precio * (1 - STOP_LOSS),
        "take_profit": precio * (1 + TAKE_PROFIT),
        "riesgo_teorico_usd": riesgo,
        "beneficio_teorico_usd": beneficio,
        "riesgo_autorizado": riesgo <= RIESGO_MAXIMO_TEORICO_USD,
    }
