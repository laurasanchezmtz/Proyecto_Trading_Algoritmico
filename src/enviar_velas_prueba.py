
import json
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

productor = Producer({"bootstrap.servers": "localhost:9092"})

# Inicio de las velas de prueba
inicio = datetime.now(timezone.utc).replace(
    second=0, microsecond=0
) - timedelta(minutes=60)

# Generar 60 velas consecutivas con precios ascendentes
for i in range(60):
    precio = round(1.0800 + i * 0.0001, 5)
    fecha = inicio + timedelta(minutes=i)

    vela = {
        "symbol": "EURUSD",
        "inicio": fecha.isoformat().replace("+00:00", "Z"),
        "open": precio,
        "high": precio,
        "low": precio,
        "close": precio,
        "num_ticks": 1
    }

    productor.produce(
        "velas_prueba",
        value=json.dumps(vela).encode("utf-8")
    )

# Confirmar que todos los mensajes salieron de la cola local
pendientes = productor.flush(10)

if pendientes:
    raise RuntimeError(
        f"No se confirmó el envío de {pendientes} mensajes"
    )

print("60 velas ficticias enviadas a velas_prueba")
