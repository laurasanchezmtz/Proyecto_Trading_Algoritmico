
import json
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer

# Conectar con Kafka
productor = Producer({
    "bootstrap.servers": "localhost:9092"
})

# Fecha actual para evitar señales antiguas
base = datetime.now(timezone.utc)

def fecha(numero):
    momento = base + timedelta(milliseconds=numero)
    return momento.isoformat().replace("+00:00", "Z")

def crear_senal(tipo, simbolo, precio, timestamp):
    return {
        "id_algoritmo": "SMA_10_50",
        "symbol": simbolo,
        "signal": tipo,
        "precio_referencia": precio,
        "timestamp": timestamp
    }

# Primera señal válida
compra = crear_senal(
    "BUY", "EURUSD", 1.0850, fecha(1)
)

# Seis escenarios de prueba
pruebas = [
    ("Compra válida", compra),

    ("Compra duplicada", compra.copy()),

    ("Compra con posición abierta",
     crear_senal("BUY", "EURUSD", 1.0852, fecha(2))),

    ("Venta válida",
     crear_senal("SELL", "EURUSD", 1.0860, fecha(3))),

    ("Venta sin posición",
     crear_senal("SELL", "EURUSD", 1.0861, fecha(4))),

    ("Símbolo no autorizado",
     crear_senal("BUY", "GBPUSD", 1.2700, fecha(5)))
]

# Enviar las pruebas en orden
for numero, (nombre, senal) in enumerate(pruebas, 1):

    productor.produce(
        "signals_ejecutor_prueba",
        value=json.dumps(senal).encode("utf-8")
    )

    pendientes = productor.flush(10)

    if pendientes:
        raise RuntimeError(
            f"No se confirmó el envío de la prueba {numero}"
        )

    print(f"{numero}. Enviada: {nombre}")

print("\nSe enviaron las 6 señales de prueba.")
