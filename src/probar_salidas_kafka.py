"""Prueba aislada: Kafka velas_prueba -> EstrategiaSMA -> signals_prueba.

No importa ni ejecuta órdenes de Interactive Brokers.
Ejecutar desde la raíz: python src/probar_salidas_kafka.py
"""
import json
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from confluent_kafka import Consumer, Producer, TopicPartition
from estrategia_sma import EstrategiaSMA

SERVIDOR = "localhost:9092"
TOPICO_VELAS = "velas_prueba"
TOPICO_SENALES = "signals_prueba"

# Los tres casos empiezan con 50 cierres ascendentes: BUY a 1.0849.
subida = [round(1.0800 + i * 0.0001, 5) for i in range(50)]
casos = [
    ("STOP_LOSS", subida + [round(1.0849 * 0.97, 5)]),
    ("TAKE_PROFIT", subida + [round(1.0849 * 1.05, 5)]),
    ("CRUCE_BAJISTA", subida + [round(1.0849 - i * 0.0002, 5) for i in range(1, 40)]),
]

productor = Producer({"bootstrap.servers": SERVIDOR})
consumidor = Consumer({
    "bootstrap.servers": SERVIDOR,
    "group.id": "prueba-salidas-" + uuid4().hex[:8],
    "enable.auto.commit": False,
})

try:
    for numero, (motivo_esperado, cierres) in enumerate(casos):
        # Comenzar en el final del tópico: ignorar pruebas anteriores.
        particion = TopicPartition(TOPICO_VELAS, 0)
        _, offset_final = consumidor.get_watermark_offsets(particion, timeout=10)
        consumidor.assign([TopicPartition(TOPICO_VELAS, 0, offset_final)])

        # Los tiempos son ficticios y no necesitan coincidir con el reloj.
        inicio = (
            datetime.now(timezone.utc).replace(second=0, microsecond=0)
            - timedelta(hours=12) + timedelta(minutes=numero * 120)
        )
        for i, cierre in enumerate(cierres):
            vela = {
                "symbol": "EURUSD",
                "inicio": (inicio + timedelta(minutes=i)).isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z"),
                "open": cierre,
                "high": cierre,
                "low": cierre,
                "close": cierre,
                "num_ticks": 1,
            }
            productor.produce(TOPICO_VELAS, value=json.dumps(vela).encode("utf-8"))

        if productor.flush(20):
            raise RuntimeError("No terminaron de enviarse todas las velas")

        estrategia = EstrategiaSMA()
        senales = []
        recibidos = 0
        limite = time.monotonic() + 45
        while recibidos < len(cierres):
            if time.monotonic() > limite:
                raise TimeoutError(
                    f"Solo se recibieron {recibidos}/{len(cierres)} velas de {motivo_esperado}"
                )
            mensaje = consumidor.poll(1)
            if mensaje is None:
                continue
            if mensaje.error():
                raise RuntimeError(str(mensaje.error()))

            vela = json.loads(mensaje.value().decode("utf-8"))
            recibidos += 1
            senal = estrategia.procesar_vela(vela)
            if senal:
                senales.append((senal["signal"], estrategia.ultimo_motivo))
                productor.produce(
                    TOPICO_SENALES,
                    value=json.dumps(senal).encode("utf-8"),
                )
                print(
                    f"  {senal['signal']} a {senal['precio_referencia']} "
                    f"(motivo: {estrategia.ultimo_motivo})"
                )

        if productor.flush(20):
            raise RuntimeError("No terminaron de enviarse todas las señales")

        esperado = [("BUY", "SMA_CORTA_MAYOR"), ("SELL", motivo_esperado)]
        if senales != esperado:
            raise AssertionError(f"{motivo_esperado}: esperado {esperado}, recibido {senales}")
        print(f"OK: {motivo_esperado} ({recibidos} velas, 1 BUY y 1 SELL)\n")

    print("RESULTADO: 3/3 condiciones SELL comprobadas en Kafka de pruebas.")
    print("No se enviaron órdenes a Interactive Brokers.")
finally:
    consumidor.close()
