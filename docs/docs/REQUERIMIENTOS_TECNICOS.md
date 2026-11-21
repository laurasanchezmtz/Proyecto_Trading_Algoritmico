# Requerimientos técnicos y entorno de desarrollo

**Fecha del corte:** 21/11/2026  
**Equipo ejecutor:** Windows 11, aproximadamente 8 GB RAM  
**Objetivo:** indicar qué se necesita, qué funciona hoy y cómo repetir las pruebas existentes.

**Para instalar desde cero en otra computadora:** seguir primero **[INSTALACION_PASO_A_PASO.md](INSTALACION_PASO_A_PASO.md)**. Este documento es un inventario técnico, no sustituye la guía de instalación. Los integrantes que solo siguen pantalla no necesitan instalar las herramientas.

## 1. Requisitos solicitados por el profesor

| Elemento | Papel en el proyecto | Estado |
|---|---|---|
| Interactive Brokers (IBKR) demo/paper | Fuente de precios y destino de órdenes **simuladas** | Cuenta creada; **pendiente de verificación y acceso API** |
| Mercado FOREX | Mercado en que debe operar el algoritmo | EURUSD seleccionado provisionalmente |
| Apache Kafka | Recibe eventos en `ticks`, `velas` y `signals` | Funcionando |
| Apache Spark Structured Streaming | Transforma ticks en velas de 1 minuto | Motor y lectura verificados; transformación pendiente |
| Algoritmo de trading | Consume ticks o velas y publica BUY/SELL | Pendiente |
| Ejecutor con gestión de riesgo | Revisa señales y envía órdenes a cuenta paper | Pendiente |
| Backtesting | Prueba con datos históricos | Deseable, no obligatorio |

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
| Cliente Kafka Python | Librería `confluent-kafka` instalada y funcional; **versión exacta no registrada** |

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

Instalación de la única dependencia Python utilizada hasta este corte:

```powershell
python -m pip install -r requirements.txt
python -m pip show confluent-kafka
```

Para una computadora nueva, primero ejecutar `python -m venv .venv` desde la raíz y después activar `.venv` (véase la guía). No se ha necesitado instalar `pyspark` en Windows: Spark se ejecuta dentro de su imagen Docker. La versión exacta de `confluent-kafka` debe registrarse posteriormente si quieren un archivo de dependencias fijado a versiones concretas.

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

**Resultado comprobado:** Spark mostró **11 mensajes JSON** del tópico `ticks` en consola. El código solo realiza **lectura**; todavía no transforma ni escribe velas.

**Limitación de reproducibilidad:** como se usa `--rm`, el contenedor de Spark se elimina al terminar. La primera descarga de dependencias puede repetirse. Más adelante se propondrá un volumen persistente y una red Docker propia/Compose, probados con los recursos de esta computadora.

## 7. Interactive Brokers: trabajo pendiente

- Cuenta paper creada por un integrante, **verificación pendiente**.
- No se ha verificado que la API permita conectarse ni que la cuenta tenga permisos de datos de FOREX.
- No está instalado/configurado ni probado TWS o IB Gateway como parte de las evidencias de este corte.
- La conexión futura deberá confirmarse **en modo paper**, nunca en una cuenta de operaciones reales.
- Documentación que compartió el profesor: https://interactivebrokers.github.io/tws-api/

**Seguridad:** no subir claves, contraseñas, archivos `.env`, identificadores sensibles de cuenta ni imágenes de TWS que revelen datos privados. No habilitar órdenes reales en este proyecto.

## 8. Pendientes técnicos antes de integrar todo

1. Pasar de la red temporal compartida a una configuración robusta entre servicios Kafka/Spark.
2. Resolver persistencia de dependencias Spark y plan de límites de memoria.
3. Diseñar y validar ventanas de evento de un minuto para OHLC, incluidos tiempos y posibles mensajes tardíos.
4. Crear publicación desde Spark al tópico `velas` respetando el esquema del profesor.
5. Implementar estado de posiciones, medias móviles, señales y deduplicación.
6. Definir tamaño de operación y límites de riesgo antes de conectar el ejecutor a IBKR.
7. Registrar versiones exactas de dependencias y evidencias de pruebas.
