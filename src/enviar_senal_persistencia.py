
import json
import sys
from datetime import datetime, timezone
from confluent_kafka import Producer

# Elegir BUY o SELL desde la terminal
if len(sys.argv) != 2 or sys.argv[1].upper() not in ("BUY", "SELL"):
    raise SystemExit("Uso: python src/enviar_senal_persistencia.py BUY|SELL")

tipo = sys.argv[1].upper()

productor = Producer({
    "bootstrap.servers": "localhost:9092"
})

senal = {
    "id_algoritmo": "SMA_10_50",
    "symbol": "EURUSD",
    "signal": tipo,
    "precio_referencia": 1.0850,
    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
}

# Enviar únicamente al tópico de pruebas
productor.produce(
    "signals_ejecutor_prueba",
    value=json.dumps(senal).encode("utf-8")
)

if productor.flush(10) != 0:
    raise RuntimeError("No se pudo completar el envío")

print(f"Señal {tipo} enviada al tópico de pruebas")
