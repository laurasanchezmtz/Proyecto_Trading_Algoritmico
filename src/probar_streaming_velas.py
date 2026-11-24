
import json
import time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

productor = Producer({"bootstrap.servers": "localhost:9092"})

# Crear tres ticks pertenecientes al mismo minuto anterior
base = (datetime.now(timezone.utc) - timedelta(minutes=3))
base = base.replace(second=0, microsecond=0)

for segundo, bid in [(5, 1.0850), (15, 1.0852), (25, 1.0854)]:
    fecha = base + timedelta(seconds=segundo)

    mensaje = {
        "symbol": "EURUSD",
        "bid": bid,
        "ask": round(bid + 0.0002, 5),
        "timestamp": fecha.isoformat().replace("+00:00", "Z")
    }

    productor.produce("ticks", json.dumps(mensaje))

if productor.flush(10) != 0:
    raise RuntimeError("No se confirmó el envío de los ticks")

print("Tres cotizaciones enviadas.")

# Separar las cotizaciones en distintos ciclos de Spark
time.sleep(7)

# Enviar un tick actual para avanzar el watermark
mensaje_actual = {
    "symbol": "EURUSD",
    "bid": 1.0860,
    "ask": 1.0862,
    "timestamp": datetime.now(timezone.utc)
        .isoformat().replace("+00:00", "Z")
}

productor.produce("ticks", json.dumps(mensaje_actual))

if productor.flush(10) != 0:
    raise RuntimeError("No se confirmó el último tick")

print("Cotización actual enviada.")
print("Revisa el consumidor del tópico velas.")
