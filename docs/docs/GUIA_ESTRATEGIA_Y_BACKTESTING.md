# Estrategia SMA 10/50 y backtesting (pasos 16, 17 y 21)

**Responsable:** Laura M. Sánchez Martínez · **Estado:** código implementado y probado con datos ficticios. Integración mediante los tópicos velas y signals establecida en código; validación integral pendiente.

## 1. Qué archivos se agregan

| Archivo | Qué hace | ¿Necesita Kafka? |
|---|---|---|
| `src/estrategia_sma.py` | La lógica de la estrategia: recibe una vela y responde con nada o con una señal BUY/SELL | No |
| `src/estrategia_kafka.py` | Conecta la estrategia con Kafka: lee `velas` y publica en `signals` | Sí |
| `src/generar_velas_prueba.py` | Crea velas ficticias reproducibles (paso 16) | No |
| `src/descargar_datos.py` | Descarga velas históricas reales de EURUSD/GBPUSD desde Yahoo Finance | No (sí internet) |
| `src/backtesting.py` | Simula la estrategia sobre datos pasados y calcula métricas (paso 21) | No |
| `tests/test_estrategia_sma.py` | 10 pruebas automáticas con resultados conocidos | No |

**Idea central:** la lógica (`estrategia_sma.py`) está separada de Kafka. Por eso la misma clase se usa en vivo y en el backtesting, y otro equipo puede correrla sobre su plataforma mientras respete los formatos acordados.

## 2. Cómo correrlo (desde la raíz del proyecto, con `.venv` activado)

```powershell
python -m pip install -r requirements.txt

# 1) Pruebas automáticas: deben salir 10 "ok"
python -m unittest discover tests -v

# 2) Backtesting con datos ficticios
python src/generar_velas_prueba.py
python src/backtesting.py --datos datos_prueba/velas_sinteticas.csv

# 3) Backtesting con datos reales (últimos 7 días, velas de 1 minuto)
python src/descargar_datos.py
python src/backtesting.py --datos datos_prueba/EURUSD_1m_7d.csv

# 4) En vivo, cuando Spark ya publique en `velas` (Kafka encendido)
python src/estrategia_kafka.py
```

Los resultados quedan en `resultados/`: `operaciones.csv`, `resumen.txt` y `grafica.png`.

Para ver la estrategia publicando señales **sin esperar a Spark**, se pueden mandar velas de prueba a mano al tópico `velas` (se necesitan al menos 50 para la primera señal).

## 3. Cómo funciona la estrategia, paso a paso

Por cada vela que llega:

1. Si es de otro símbolo, se ignora.
2. Si su `inicio` es igual o anterior a la última vela procesada, se ignora (Kafka puede reenviar mensajes).
3. Se guarda el precio de cierre. Solo se conservan los últimos 50.
4. Si todavía no hay 50 cierres, se espera.
5. Se calculan **SMA corta** (promedio de los últimos 10 cierres) y **SMA larga** (promedio de los últimos 50).
6. **Sin posición abierta:** si SMA corta > SMA larga → **BUY**.
7. **Con posición abierta:** se vende (**SELL**) si ocurre lo primero de esto:
   - el precio cayó 2 % desde la entrada (**Stop Loss**);
   - el precio subió 4 % (**Take Profit**);
   - SMA corta < SMA larga (**cruce bajista**).

La señal sale con el formato acordado: `{id_algoritmo, symbol, signal, precio_referencia, timestamp}`. El `timestamp` es el **cierre** de la vela, porque es el momento en que ya conocemos su precio final.

## 4. Resultados con datos ficticios (2000 velas, semilla 42)

| Métrica | Valor |
|---|---|
| Operaciones | 28 |
| % ganadoras | 17.9 % |
| Pips totales | −34.4 |
| Costo total por spread | 28 pips |
| Salidas por motivo | 27 por cruce bajista, 1 por fin de datos |
| Comprar y mantener (comparación) | −62.2 pips |

**Cómo leerlo:** la estrategia pierde poco en muchas operaciones (cada salida por cruce llega tarde) y gana mucho en pocas (cuando hay una tendencia larga). Casi todo lo que se pierde se explica por el spread. **Estos son datos inventados: no demuestran que la estrategia gane o pierda en la realidad.** Para eso sirve el paso 3 de la sección 2.

## 5. Hallazgos y decisiones para comentar con el equipo

1. **El Stop Loss de 2 % casi nunca se activa en FOREX con velas de 1 minuto.** En EURUSD, 2 % son unos 216 pips; en un día normal el par se mueve entre 50 y 100. En la práctica la estrategia siempre sale por cruce bajista. Se puede probar algo más realista con, por ejemplo, `--stop-loss 0.002 --take-profit 0.004` (0.2 % y 0.4 %).
2. **"Comparar medias" frente a "detectar el cruce".** El pseudocódigo compra siempre que SMA corta > SMA larga. Después de un Stop Loss, eso puede provocar una recompra inmediata. La opción `--solo-cruce` compra únicamente en la vela del cruce. Con los parámetros originales ambas dan el mismo resultado, justamente porque el Stop Loss nunca se activa. Con un Stop Loss más chico sí cambian.
3. **SELL significa "cerrar la compra", no "vender en corto".** La estrategia solo abre compras. Hay que confirmar que el ejecutor de nuestro equipo y el de los demás lo interpreten igual: si llega un SELL sin posición abierta, debe rechazarse y no abrir un corto.
4. **El precio de las velas** (`mid`, bid o ask) lo decide el job de Spark. La estrategia solo usa `close`.

## 6. Supuestos del backtesting (decirlos en la exposición)

- Se opera al cierre de la vela que generó la señal.
- Se cobra el spread (1 pip por defecto): se compra medio pip más caro y se vende medio pip más barato.
- No se incluyen comisiones ni swaps.
- El capital es fijo: 1000 USD por operación, como en el pseudocódigo.
- Los datos de Yahoo son gratuitos y aproximados, sin bid/ask. IBKR ofrece históricos cuando la cuenta esté verificada.
