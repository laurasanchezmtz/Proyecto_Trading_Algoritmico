
import json
import math
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from confluent_kafka import Consumer

# Base de datos local del proyecto
ruta_bd = (
    Path(__file__).resolve().parent.parent
    / "datos_prueba" / "ejecutor_prueba.sqlite3"
)

ruta_bd.parent.mkdir(parents=True, exist_ok=True)
db = sqlite3.connect(ruta_bd)

# Crear las tablas si todavía no existen
with db:
    db.execute("""
        CREATE TABLE IF NOT EXISTS estado (
            id INTEGER PRIMARY KEY,
            en_posicion INTEGER NOT NULL
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS aceptadas (
            clave TEXT PRIMARY KEY,
            tipo TEXT NOT NULL,
            precio REAL NOT NULL
        )
    """)

    db.execute(
        "INSERT OR IGNORE INTO estado VALUES (1, 0)"
    )


def posicion_abierta():
    fila = db.execute(
        "SELECT en_posicion FROM estado WHERE id = 1"
    ).fetchone()
    return bool(fila[0])


def validar(s):
    if not isinstance(s, dict):
        return "Mensaje inválido"

    campos = (
        "id_algoritmo", "symbol", "signal",
        "precio_referencia", "timestamp"
    )
    if any(c not in s for c in campos):
        return "Faltan campos obligatorios"

    if not isinstance(s["id_algoritmo"], str) or not s["id_algoritmo"]:
        return "Algoritmo inválido"

    if s["symbol"] != "EURUSD":
        return "Símbolo no autorizado"

    if s["signal"] not in ("BUY", "SELL"):
        return "Señal inválida"

    try:
        precio = float(s["precio_referencia"])
        fecha = datetime.fromisoformat(
            s["timestamp"].replace("Z", "+00:00")
        )

        if not math.isfinite(precio) or precio <= 0:
            return "Precio inválido"

        if fecha.tzinfo is None:
            return "Fecha sin zona horaria"

        diferencia = (
            datetime.now(timezone.utc) - fecha
        ).total_seconds()

        if diferencia > 120 or diferencia < -30:
            return "Señal antigua o futura"

    except (TypeError, ValueError, AttributeError):
        return "Precio o fecha incorrectos"

    return None


def procesar(s):
    error = validar(s)
    if error:
        return f"[RECHAZADA] {error}"

    clave = json.dumps([
        s["id_algoritmo"], s["symbol"],
        s["signal"], s["timestamp"]
    ])

    tipo = s["signal"]
    precio = float(s["precio_referencia"])

    # Guardar posición y señal en una misma transacción
    with db:
        duplicada = db.execute(
            "SELECT 1 FROM aceptadas WHERE clave = ?",
            (clave,)
        ).fetchone()

        if duplicada:
            return "[RECHAZADA] Señal duplicada"

        abierta = posicion_abierta()

        if tipo == "BUY" and abierta:
            return "[RECHAZADA] Ya existe una posición"

        if tipo == "SELL" and not abierta:
            return "[RECHAZADA] No existe posición para vender"

        db.execute(
            "INSERT INTO aceptadas VALUES (?, ?, ?)",
            (clave, tipo, precio)
        )

        nuevo_estado = 1 if tipo == "BUY" else 0

        db.execute(
            "UPDATE estado SET en_posicion = ? WHERE id = 1",
            (nuevo_estado,)
        )

    return f"[DRY-RUN] {tipo} aceptada a {precio}"


# Escuchar solo las señales de prueba
consumidor = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "ejecutor_persistente_prueba_v1",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False
})

consumidor.subscribe(["signals_ejecutor_prueba"])

print("Ejecutor persistente iniciado (DRY-RUN)")
print("Posición guardada:", posicion_abierta())
print("Esperando señales... Ctrl + C para salir")

try:
    while True:
        mensaje = consumidor.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            print("Error de Kafka:", mensaje.error())
            continue

        try:
            senal = json.loads(mensaje.value())
            resultado = procesar(senal)
            print(resultado, flush=True)

            # Confirmar la lectura después de guardar el estado
            consumidor.commit(
                message=mensaje,
                asynchronous=False
            )

        except (ValueError, UnicodeDecodeError):
            print("[RECHAZADA] JSON incorrecto")
            consumidor.commit(
                message=mensaje,
                asynchronous=False
            )

        except Exception as error:
            print("Error de procesamiento:", error)
            # No confirmar el mensaje si falla la base de datos

except KeyboardInterrupt:
    print("\nEjecutor detenido")

finally:
    consumidor.close()
    db.close()
