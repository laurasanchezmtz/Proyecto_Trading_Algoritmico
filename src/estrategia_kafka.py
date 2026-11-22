"""
Conecta la estrategia SMA con Kafka:  tópico `velas`  ->  EstrategiaSMA  ->  tópico `signals`

Uso (con Kafka encendido, desde la raíz del proyecto):
  python src/estrategia_kafka.py

Detener con Ctrl + C.
"""

import json

from confluent_kafka import Consumer, Producer

from estrategia_sma import EstrategiaSMA

SERVIDOR = "localhost:9092"

consumidor = Consumer({
    "bootstrap.servers": SERVIDOR,
    "group.id": "estrategia_sma_10_50",   # Kafka recuerda hasta dónde leyó este grupo
    "auto.offset.reset": "earliest",       # la primera vez, leer desde el inicio del tópico
})
productor = Producer({"bootstrap.servers": SERVIDOR})
estrategia = EstrategiaSMA()

consumidor.subscribe(["velas"])
print("Esperando velas en el tópico 'velas'... (Ctrl + C para salir)")

try:
    while True:
        mensaje = consumidor.poll(1.0)     # esperar hasta 1 segundo por una vela
        if mensaje is None:
            continue
        if mensaje.error():
            print("Error de Kafka:", mensaje.error())
            continue

        try:
            vela = json.loads(mensaje.value())
            senal = estrategia.procesar_vela(vela)
        except (ValueError, KeyError) as error:
            print("Vela con formato incorrecto, se ignora:", error)
            continue

        print(f"Vela {vela['inicio']} close={vela['close']} "
              f"({len(estrategia.cierres)}/{estrategia.periodo_largo} velas en memoria)")

        if senal:
            productor.produce("signals", value=json.dumps(senal))
            productor.flush(10)
            print(f"  -> SEÑAL publicada: {senal}  motivo={estrategia.ultimo_motivo}")
except KeyboardInterrupt:
    print("Estrategia detenida por el usuario.")
finally:
    consumidor.close()
