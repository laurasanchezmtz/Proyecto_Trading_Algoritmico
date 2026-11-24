
import json
import math
from datetime import datetime, timezone
from confluent_kafka import Consumer

# Configuración del consumidor de pruebas
consumidor = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "ejecutor_riesgo_prueba_v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

# Estado simulado: no hay posiciones abiertas
en_posicion = False
senales_procesadas = set()


def validar_senal(senal):
    if not isinstance(senal, dict):
        return "El mensaje no es un objeto JSON"

    campos = [
        "id_algoritmo", "symbol", "signal",
        "precio_referencia", "timestamp"
    ]

    if any(c not in senal for c in campos):
        return "Faltan campos obligatorios"

    if not isinstance(senal["id_algoritmo"], str) or not senal["id_algoritmo"]:
        return "Identificador de algoritmo inválido"

    if senal["symbol"] != "EURUSD":
        return "Símbolo no autorizado"

    if senal["signal"] not in ("BUY", "SELL"):
        return "Señal inválida"

    try:
        precio = float(senal["precio_referencia"])
        if not math.isfinite(precio) or precio <= 0:
            return "Precio inválido"

        fecha = datetime.fromisoformat(
            senal["timestamp"].replace("Z", "+00:00")
        )

        if fecha.tzinfo is None:
            return "Fecha sin zona horaria"

        diferencia = (
            datetime.now(timezone.utc) - fecha
        ).total_seconds()

        if diferencia > 120 or diferencia < -30:
            return "Señal antigua o con fecha futura"

    except (ValueError, TypeError, AttributeError):
        return "Precio o fecha incorrectos"

    return None


def procesar_senal(senal):
    global en_posicion

    error = validar_senal(senal)

    if error:
        print(f"[RECHAZADA] {error}", flush=True)
        return

    tipo = senal["signal"]
    precio = senal["precio_referencia"]

    clave = (
        senal["id_algoritmo"],
        senal["symbol"],
        tipo,
        senal["timestamp"]
    )

    if clave in senales_procesadas:
        print("[RECHAZADA] Señal duplicada", flush=True)
        return

    if tipo == "BUY" and en_posicion:
        print("[RECHAZADA] Ya existe una posición", flush=True)
        return

    if tipo == "SELL" and not en_posicion:
        print("[RECHAZADA] No existe posición para vender", flush=True)
        return

    # Simulación: no se envían órdenes a Interactive Brokers
    en_posicion = (tipo == "BUY")
    senales_procesadas.add(clave)

    print(
        f"[DRY-RUN] {tipo} aceptada a {precio}. "
        f"Posición abierta: {en_posicion}",
        flush=True
    )


consumidor.subscribe(["signals_ejecutor_prueba"])

print("Ejecutor iniciado en MODO PRUEBA")
print("Esperando señales... (Ctrl + C para salir)")

try:
    while True:
        mensaje = consumidor.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            print("Error de Kafka:", mensaje.error())
            continue

        try:
            senal = json.loads(mensaje.value().decode("utf-8"))
            procesar_senal(senal)
        except (ValueError, UnicodeDecodeError) as error:
            print("[RECHAZADA] JSON incorrecto:", error)

        consumidor.commit(message=mensaje, asynchronous=False)

except KeyboardInterrupt:
    print("\nEjecutor detenido")

finally:
    consumidor.close()
