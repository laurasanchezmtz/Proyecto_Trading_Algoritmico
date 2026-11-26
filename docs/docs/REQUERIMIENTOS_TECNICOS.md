# Requerimientos técnicos y entorno de desarrollo

**Fecha del corte:** 26/11/2026
**Versión documental:** 1.2
**Entornos:** equipo inicial Windows 11 con aproximadamente 8 GB RAM para Kafka/Spark y equipo independiente Windows para pruebas IBKR.
**Objetivo:** indicar qué se necesita, qué funciona hoy y cómo repetir las pruebas existentes.

**Para instalar desde cero en otra computadora:** seguir primero **[INSTALACION_PASO_A_PASO.md](INSTALACION_PASO_A_PASO.md)**. Este documento es un inventario técnico, no sustituye la guía de instalación. Los integrantes que solo siguen pantalla no necesitan instalar las herramientas.

## 1. Requisitos solicitados por el profesor

| Elemento | Papel en el proyecto | Estado |
|---|---|---|
| Interactive Brokers (IBKR) demo/paper | Fuente de precios y destino de órdenes **simuladas** | Cuenta Paper, acceso API y cotizaciones verificados |
| Mercado FOREX | Mercado en que debe operar el algoritmo | EURUSD seleccionado provisionalmente |
| Apache Kafka | Recibe eventos en `ticks`, `velas` y `signals` | Funcionando |
| Apache Spark Structured Streaming | Transforma ticks en velas de 1 minuto | Lectura verificada; transformación OHLC implementada, validación integral pendiente |
| Algoritmo de trading | Consume ticks o velas y publica BUY/SELL | SMA 10/50 implementada |
| Ejecutor con gestión de riesgo | Revisa señales y envía órdenes a cuenta paper | Implementado en Dry Run; órdenes pendientes |
| Backtesting | Prueba con datos históricos | Implementado; deseable, no obligatorio |

**No obligatorio en el enunciado:** Windows 11, Python, Docker, VS Code, Git, GitHub, `confluent-kafka`. Estas son decisiones de implementación del equipo.

## 2. Equipo y software verificados

| Recurso / herramienta | Valor comprobado |
|---|---|
| Sistema operativo | Windows 11 |
| Python | 3.11.9 |
| VS Code | 1.145.1 (x64) |
| Docker Engine / CLI | 29.8.0 |
| Docker Compose | v5.5.1 (instalado, **no usado todavía**) |
| WSL | WSL 2 con distribución `docker-desktop` |
| Git | 2.54.0.windows.1 |
| RAM física aproximada | 8 GB (Windows informó 8,260,263,936 bytes) |
| RAM reportada por el motor Docker | 3.673 GiB |
| CPU visibles por Docker | 12 |
| Apache Kafka | Imagen `apache/kafka:4.4.0` |
| Apache Spark | Imagen `apache/spark:4.2.1-scala2.13-java17-python3-ubuntu` |
| Cliente Kafka Python | confluent-kafka==2.16.2 |
| Procesamiento de datos | pandas==3.1.1 |
| Visualización | matplotlib==3.11.2 |
| Datos históricos | yfinance==1.8.0 |
| API Interactive Brokers | ib_async==2.1.0 |

**Recursos:** en la medición del 21/11/2026, el contenedor Kafka utilizó **608.6 MiB** de memoria y **3.70 % de CPU**. Esta es una medición puntual, no una garantía de consumo constante. Con aproximadamente 8 GB físicos de RAM, se debe probar Spark en modo local y evitar lanzar múltiples procesos grandes simultáneamente.

## 3. Carpetas y entorno virtual

En la computadora ejecutora el proyecto está en:

```powershell
C:\Users\maria\Desktop\Proyecto_Trading_Algoritmico
```

En la computadora ejecutora el entorno `.venv` **ya existe**. Para trabajar desde la carpeta raíz en **PowerShell**:

```powershell
cd C:\Users\maria\Desktop\Proyecto_Trading_Algoritmico
.\.venv\Scripts\Activate.ps1
python --version
```

**Entorno virtual (`.venv`):** instalación aislada de librerías para este proyecto. Se detectó otra carpeta `venv/`; no subir ninguna de las dos al repositorio.

Instalación de las dependencias Python registradas en requirements.txt:

```powershell
python -m pip install -r requirements.txt
python -m pip show confluent-kafka
```

Para una computadora nueva, primero ejecutar `python -m venv .venv` desde la raíz y después activar `.venv` (véase la guía). No se ha necesitado instalar `pyspark` en Windows: Spark se ejecuta dentro de su imagen Docker. Las cinco dependencias directas fueron verificadas y fijadas en requirements.txt el 26/11/2026. La comprobación mediante pip check no detectó incompatibilidades en el entorno local.

## 4. Kafka: cómo revisar el entorno

Kafka se creó con un contenedor local llamado `kafka-trading` y el puerto `9092` publicado a Windows:

```powershell
docker ps
docker exec kafka-trading /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Deben aparecer los tres tópicos:

```text
signals
ticks
velas
```

Si el contenedor **ya existe pero está detenido**, puede iniciarse con:

```powershell
docker start kafka-trading
```

**No ejecutar de nuevo `docker run --name kafka-trading ...` si el contenedor ya existe**, porque Docker rechazará duplicar el nombre. No borrar ni recrear el contenedor mientras dependamos de sus mensajes de prueba.

Para inspeccionar su consumo:

```powershell
docker stats --no-stream kafka-trading
```

## 5. Prueba del productor Python

Desde la **raíz** del proyecto, con Kafka activo y `.venv` activado:

```powershell
python src/productor_ticks.py
```

El programa utilizado hasta este corte genera cinco cotizaciones **ficticias**, en intervalos de un segundo. No hay conexión con IBKR.

Para inspeccionar los mensajes de `ticks`:

```powershell
docker exec -it kafka-trading /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ticks --from-beginning
```

`--from-beginning` lee también mensajes históricos todavía retenidos por Kafka. Para finalizar el consumidor: `Ctrl + C`.

## 6. Prueba de lectura de Spark

Desde la **raíz del proyecto**, en PowerShell, con el contenedor `kafka-trading` activo:

```powershell
docker run --rm --network container:kafka-trading --memory=1792m --cpus=2 -e HOME=/tmp -v "${PWD}\src:/app:ro" --entrypoint /opt/spark/bin/spark-submit apache/spark:4.2.1-scala2.13-java17-python3-ubuntu --master "local[1]" --driver-memory 768m --conf spark.ui.enabled=false --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 /app/leer_ticks_spark.py
```

**Qué significa:**

- `--network container:kafka-trading`: Spark comparte temporalmente la red de Kafka. Es una **solución para pruebas**, no la arquitectura final entre servicios.
- `--memory=1792m`, `--cpus=2`, `--driver-memory 768m`: límites de recursos para la prueba.
- `-v "${PWD}\src:/app:ro"`: pone los scripts `src` a disposición del contenedor, en modo solo lectura.
- `--packages ...`: incorpora el conector de Spark con Kafka.
- `-e HOME=/tmp` y `spark.jars.ivy=/tmp/spark-ivy`: solucionan el error observado cuando Ivy intentó escribir en `/nonexistent/.ivy2.5.2/cache`.

**Resultado comprobado:** Spark mostró **11 mensajes JSON** del tópico `ticks` en consola. El script leer_ticks_spark.py solo realiza lectura. Posteriormente se incorporó la transformación OHLC en publicar_velas_spark.py, cuya validación integral permanece pendiente.

**Limitación de reproducibilidad:** como se usa `--rm`, el contenedor de Spark se elimina al terminar. La primera descarga de dependencias puede repetirse. Más adelante se propondrá un volumen persistente y una red Docker propia/Compose, probados con los recursos de esta computadora.

## 7. Interactive Brokers: configuración y seguridad

| Requerimiento | Configuración |
|---|---|
| Aplicación | Trader Workstation (TWS) |
| Sesión | Exclusivamente Paper Trading |
| Biblioteca Python | ib_async==2.1.0 |
| Host | 127.0.0.1 |
| Puerto API | 7497 |
| Permisos API | Solo lectura |
| Conexiones permitidas | Localhost |
| Client ID del ejecutor | 31 |
| Cuenta autorizada | Configuración local mediante .env |
| Modalidad del ejecutor | Dry Run |

La conexión API y la recepción de cotizaciones EURUSD
fueron verificadas el 24/11/2026.

El 26/11/2026 se comprobó el inicio autorizado del ejecutor,
el bloqueo automático ante el cierre de TWS y el rechazo
de inicio cuando TWS no está disponible.

La configuración debe mantenerse en solo lectura.

Documentación oficial proporcionada por el profesor:
https://interactivebrokers.github.io/tws-api/

Consultar también:
[Integración API IBKR](../INTEGRACION_API_IBKR.md).

## 8. Requerimientos del ejecutor de riesgo

### 8.1. Componentes

| Archivo | Función |
|---|---|
| src/ejecutor_riesgo.py | Consumo de señales y evaluación de riesgos |
| src/config_riesgo.py | Parámetros de riesgo |
| src/calculo_riesgo.py | Dimensionamiento y riesgo monetario |
| src/seguridad_ibkr.py | Validación de conexión y cuenta autorizada |
| datos_prueba/ejecutor.sqlite3 | Persistencia del estado lógico |

### 8.2. Parámetros de simulación

| Parámetro | Valor |
|---|---|
| Instrumento | EURUSD |
| Exposición nominal máxima | 1000 USD |
| Riesgo monetario máximo teórico | 20 USD |
| Stop Loss | 2 % del precio |
| Take Profit | 4 % del precio |
| Posiciones lógicas simultáneas | 1 |
| Vigencia máxima de señal | 120 segundos |
| Tolerancia temporal futura | 30 segundos |
| Tópico Kafka | signals |
| Servidor Kafka | localhost:9092 |
| Grupo consumidor | ejecutor_persistente_v1 |

Los parámetros corresponden a decisiones de implementación
para simulación y no constituyen garantías de pérdida máxima.

### 8.3. Persistencia local

Se utiliza sqlite3, incluido en la biblioteca estándar de Python.
No requiere instalar un servidor de bases de datos ni una
dependencia adicional mediante pip.

La base de datos conserva las señales aceptadas y el estado
lógico de la posición entre ejecuciones.

La posición lógica no equivale necesariamente a una posición
confirmada por Interactive Brokers.

### 8.4. Configuración privada

El ejecutor requiere un archivo .env en la raíz del proyecto
con la variable IBKR_PAPER_ACCOUNT.

Cada integrante deberá configurar su propia cuenta Paper
autorizada, sin compartir identificadores privados.

El archivo .env y la base de datos local deben permanecer
excluidos del repositorio mediante .gitignore.

No se requiere python-dotenv: la configuración se lee
mediante pathlib, incluido en Python.

### 8.5. Restricciones de seguridad

- No utilizar cuentas reales.
- No habilitar permisos de escritura durante las pruebas actuales.
- No enviar órdenes mientras el ejecutor permanezca en Dry Run.
- Validar la cuenta autorizada antes de procesar señales.
- Bloquear el procesamiento ante pérdida de conexión detectada.
- No publicar credenciales, identificadores de cuenta ni archivos privados.
- No interpretar el puerto o prefijo de cuenta como prueba suficiente de Paper Trading.

## 9. Pendientes técnicos antes de integrar todo

1. Validar la configuración definitiva de red Kafka/Spark.
2. Resolver persistencia de dependencias y checkpoints de Spark.
3. Validar ventanas OHLC, eventos tardíos y duplicados.
4. Verificar publicación y consumo integrado de ticks, velas y signals.
5. Sincronizar estrategia, posición lógica y posiciones reales del entorno Paper.
6. Validar cantidades, órdenes existentes y controles de riesgo restantes.
7. Comprobar recuperación ante fallos de SQLite y offsets Kafka.
8. Comprobar cierre de recursos ante excepciones.
9. Ejecutar pruebas integradas del ejecutor Dry Run.
10. Registrar versiones exactas de dependencias y evidencias.
11. Validar restricciones de tamaño de operación del broker.
12. Realizar pruebas de órdenes exclusivamente Paper después de aprobar los controles.

## 10. Limitaciones del corte actual

Las pruebas de seguridad del ejecutor fueron realizadas
en un equipo con TWS disponible, pero sin conexión
a Kafka en localhost:9092.

No se ha comprobado el procesamiento integrado de señales
en este entorno ni se han enviado órdenes a IBKR.

El siguiente integrante deberá continuar en el paso 18.9
del mapa de trabajo, manteniendo la API en solo lectura.

Consultar:
[Mapa de trabajo](MAPA_DE_TRABAJO.md).
