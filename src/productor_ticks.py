import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

# Conectar Python con Kafka
productor = Producer({"bootstrap.servers": "localhost:9092"})

# Simular cinco cotizaciones de EURUSD
for i in range(5):

    bid = round(1.0850 + i * 0.0001, 4)
    ask = round(bid + 0.0002, 4)

    mensaje = {
        "symbol": "EURUSD",
        "bid": bid,
        "ask": ask,
        "timestamp": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
    }

    # Enviar el mensaje al tópico ticks
    productor.produce("ticks", value=json.dumps(mensaje))
    pendientes = productor.flush(10)

    if pendientes == 0:
        print("Cotización enviada:", mensaje)
    else:
        print("No se confirmó el envío.")
        break

    time.sleep(1)

print("Programa finalizado.")