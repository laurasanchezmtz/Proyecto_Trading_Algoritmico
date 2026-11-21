# Instalación paso a paso para nuevos integrantes (Windows 11)

**Proyecto:** Trading Algorítmico · **Corte:** 21/11/2026 · **Nivel:** desde cero  
**Objetivo:** preparar una computadora Windows 11 para reproducir **lo que ya funciona**: Python → Kafka (`ticks`) → lectura con Spark. **Todavía no instala una plataforma de trading completa**; velas OHLC, `signals`, ejecutor y conexión IBKR están pendientes.

> **Si solo vas a seguir la sesión con pantalla compartida:** no necesitas instalar estas herramientas. Este documento es para quien vaya a ejecutar el proyecto en su propio equipo.  
> **Si ya trabajas en la computadora de María:** salta a la [Sección B (continuar con la instalación existente)](#b-continuar-en-la-computadora-que-ya-funciona). No vuelvas a crear contenedores ni descargues imágenes sin necesidad.

## 0. Antes de empezar: palabras que usaremos

| Palabra | Explicación sencilla |
|---|---|
| **PowerShell** | Terminal de Windows donde escribimos los comandos de estas instrucciones. |
| **Repositorio (GitHub)** | Carpeta de trabajo cuyo historial se comparte con el equipo. |
| **Git** | Programa que guarda versiones del código y permite sincronizarlo con GitHub. |
| **Entorno virtual `.venv`** | Carpeta aislada para las librerías Python de este proyecto. Se crea en cada computadora; **no se descarga de GitHub**. |
| **Imagen Docker** | Paquete con los programas y archivos para iniciar un servicio. |
| **Contenedor** | Ejecución aislada de una imagen. |
| **Tópico Kafka** | Canal de mensajes: `ticks` (precios), `velas` (velas) y `signals` (decisiones). |
| **Spark** | Programa que procesa los mensajes; aquí corre dentro de Docker, no como instalación local de Windows. |

## A. Instalar por primera vez en una computadora Windows 11

### A1. Comprobar el equipo

**Necesario para repetir esta prueba:** Windows 11 de 64 bits, Internet para las descargas y suficiente espacio libre en disco para las imágenes Docker. En la computadora ejecutora se probó con **~8 GB de RAM física y 3.673 GiB disponibles para Docker**, utilizando límites de memoria para Spark. **16 GB de RAM es recomendable**, no una obligación impuesta por el profesor. No está garantizado que otra computadora de 8 GB funcione igual si ejecuta demasiados programas al mismo tiempo.

1. Abrir el menú Inicio, buscar **PowerShell** y abrirlo (no se requiere modo administrador para las verificaciones habituales).
2. Consultar la RAM física:

```powershell
[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 2)
```

**Resultado esperado:** un número en GB; en el equipo utilizado fue cerca de 8. No confundir RAM de Windows con memoria asignada a Docker.

### A2. Instalar Python 3.11

**Para qué:** ejecutar `src/productor_ticks.py`, que crea cotizaciones ficticias y las envía a Kafka.

1. Descargar el **instalador Windows de 64 bits** desde la página oficial de [Python 3.11.9](https://www.python.org/downloads/release/python-3119/).
2. Ejecutarlo. Si ofrece la opción **Add python.exe to PATH**, marcarla. Completar la instalación.
3. Cerrar y volver a abrir PowerShell para que reconozca los comandos.
4. Comprobar:

```powershell
python --version
```

**Resultado esperado:** `Python 3.11.9` (o la versión compatible acordada con el equipo). Esta guía reproduce la instalación **efectivamente probada con 3.11.9**; no obliga a actualizar Python de otras computadoras si ya tienen una versión compatible. Si `python` no se reconoce, comprobar `py --version` y la configuración PATH antes de reinstalar.

### A3. Instalar Visual Studio Code (VS Code)

**Para qué:** abrir la carpeta del proyecto, editar archivos `.py` y ejecutar comandos en una terminal integrada.

1. Descargar [Visual Studio Code para Windows (sitio oficial)](https://code.visualstudio.com/download).
2. Instalar y abrir VS Code. Se recomienda agregar la extensión **Python** de Microsoft desde la sección Extensiones.
3. Comprobar en PowerShell:

```powershell
code --version
```

**Resultado esperado:** número de versión. Si `code` no se reconoce, VS Code puede seguir funcionando desde el menú Inicio; hay que añadir el comando al PATH o volver a abrir la terminal.

### A4. Instalar Git

**Para qué:** descargar el repositorio del equipo y registrar cambios.

1. Descargar [Git for Windows (sitio oficial)](https://git-scm.com/install/windows).
2. Completar la instalación, normalmente con los valores predeterminados.
3. Abrir una **terminal nueva** y comprobar:

```powershell
git --version
```

**Resultado esperado:** una versión de Git. En la computadora ejecutora fue `2.54.0.windows.1`; no es imprescindible que todos tengan esa cifra exacta.

### A5. Instalar Docker Desktop y verificar WSL 2

**Para qué:** iniciar Apache Kafka y Apache Spark sin instalar manualmente Kafka, Spark o Java en Windows.

1. Seguir la guía oficial de [Docker Desktop para Windows](https://docs.docker.com/desktop/setup/install/windows-install/).
2. Durante la configuración, elegir o activar el motor **WSL 2** para contenedores Linux cuando la instalación lo solicite. Si WSL 2 falta, seguir las instrucciones que muestra Docker/Microsoft y reiniciar Windows si lo requiere.
3. Abrir Docker Desktop desde Inicio y esperar a que el motor indique que está funcionando.
4. Abrir PowerShell y ejecutar:

```powershell
wsl --status
docker --version
docker compose version
docker info
```

**Resultado esperado:** en `docker info` deben aparecer las secciones `Client` y `Server`. Si sale `WSL1 is not supported`, **no es necesariamente un error**: nuestro proyecto utiliza WSL 2. Si falta `Server`, abrir Docker Desktop y comprobar que el motor esté iniciado.

5. Probar un contenedor sencillo:

```powershell
docker run --rm hello-world
```

**Resultado esperado:** `Hello from Docker!`. Si aparece, Docker puede descargar y ejecutar imágenes.

> **No es necesario instalar por separado** Apache Kafka, Apache Spark, Java, WSL 1 o `pyspark` en Windows para reproducir el corte actual. Kafka y Spark se descargarán como imágenes Docker. Docker Compose está instalado pero todavía **no se usa** en este corte.

### A6. Descargar el repositorio de GitHub (después del primer corte publicado)

**Para qué:** obtener los archivos del proyecto, incluidos los scripts de la carpeta `src/`.

1. Elegir una carpeta de trabajo, por ejemplo el Escritorio, y abrir PowerShell **en esa carpeta**.
2. Escribir:

```powershell
git clone https://github.com/mariadelcarmenmatamagallanes/Proyecto_Trading_Algoritmico.git
cd .\Proyecto_Trading_Algoritmico
```

3. Abrir esa misma carpeta en VS Code: **File → Open Folder**.
4. Verificar los archivos:

```powershell
Get-ChildItem
Get-ChildItem .\src
```

**Resultado esperado:** `README.md`, `requirements.txt`, `docs/`, `src/` y los scripts `productor_ticks.py` y `leer_ticks_spark.py`.

> **Muy importante:** los dos scripts de `src/` están en la computadora ejecutora; **no se incluyen en el ZIP documental**. Deben subirse desde esa computadora en el primer commit. Si todavía no aparecen en GitHub, no podrán reproducir estas pruebas en otra computadora: soliciten al ejecutor completar ese commit. No intenten recrearlos de memoria.

### A7. Crear el entorno virtual e instalar las dependencias Python

**Para qué:** evitar mezclar las librerías de este proyecto con las de otros. **Se hace una vez por computadora**.

En PowerShell, desde la raíz de la carpeta clonada:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version
python -m pip install -r requirements.txt
python -m pip show confluent-kafka
```

**Resultados esperados:** aparece `(.venv)` antes de la ruta de PowerShell, Python funciona y `pip show` presenta información de `confluent-kafka`.

- `python -m venv .venv` crea el entorno.
- `Activate.ps1` lo activa para esa terminal.
- `pip install -r requirements.txt` instala las librerías Python indicadas en el proyecto.
- `pip show` permite comprobar la instalación.

**Si PowerShell bloquea `Activate.ps1`:** no es necesario modificar la seguridad de Windows para continuar. Pueden utilizar directamente el ejecutable del entorno sin activarlo:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip show confluent-kafka
```

Y cuando más adelante se indique `python src/productor_ticks.py`, ejecutar en su lugar ` .\.venv\Scripts\python.exe src/productor_ticks.py ` (sin los espacios que aquí delimitan la explicación).

**Nota de reproducibilidad:** la versión exacta de `confluent-kafka` usada por el ejecutor aún no fue registrada; por eso el `requirements.txt` actual no está fijado a una versión. Antes de declarar la instalación idéntica en todos los equipos, el ejecutor debe compartir el resultado de `python -m pip show confluent-kafka` y el equipo decidir si fija esa versión.

### A8. Descargar e iniciar Kafka

**Para qué:** almacenar los mensajes de precios en `ticks` para que Spark pueda leerlos.

En **PowerShell** con Docker Desktop encendido:

```powershell
docker pull apache/kafka:4.4.0
```

Solo si **no existe** un contenedor llamado `kafka-trading`, crearlo:

```powershell
docker run -d --name kafka-trading -p 9092:9092 apache/kafka:4.4.0
```

Si el contenedor **ya existe** pero está detenido, **no repetir `docker run`**; usar:

```powershell
docker start kafka-trading
```

Verificar:

```powershell
docker ps
docker exec kafka-trading /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092
```

**Resultado esperado:** `kafka-trading` aparece `Up` y se muestra `Cluster ID: ...` (cada instalación puede tener otro ID).

### A9. Crear y verificar los tópicos

**Para qué:** el profesor definió estos tres canales y sus nombres se deben conservar.

Ejecutar una sola vez en una instalación nueva:

```powershell
docker exec kafka-trading /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic ticks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka-trading /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic velas --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka-trading /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic signals --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Comprobar:

```powershell
docker exec kafka-trading /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Resultado esperado:** aparecen `ticks`, `velas` y `signals` (en cualquier orden). Los tópicos `velas` y `signals` están creados pero **todavía no contienen salidas de Spark ni del algoritmo**.

### A10. Enviar cinco cotizaciones ficticias desde Python

**Para qué:** confirmar Python → Kafka. Asegurarse de estar en la **raíz** del repositorio (la carpeta que contiene `src/`).

En la primera terminal:

```powershell
python src/productor_ticks.py
```

En otra terminal, revisar los mensajes almacenados:

```powershell
docker exec -it kafka-trading /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ticks --from-beginning
```

**Resultado esperado:** se ven **al menos cinco mensajes** JSON con `symbol`, `bid`, `ask` y `timestamp`. Pueden aparecer más porque Kafka conserva mensajes anteriores. Detener el consumidor con `Ctrl + C`; **no detiene Kafka**.

### A11. Descargar Spark y comprobar lectura de Kafka

**Para qué:** confirmar Python → Kafka → Spark con la misma configuración que funcionó en el corte.

Descargar la imagen:

```powershell
docker pull apache/spark:4.2.1-scala2.13-java17-python3-ubuntu
```

Desde la **raíz** del repositorio, ejecutar **esta sola línea completa** en PowerShell:

```powershell
docker run --rm --network container:kafka-trading --memory=1792m --cpus=2 -e HOME=/tmp -v "${PWD}\src:/app:ro" --entrypoint /opt/spark/bin/spark-submit apache/spark:4.2.1-scala2.13-java17-python3-ubuntu --master "local[1]" --driver-memory 768m --conf spark.ui.enabled=false --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 /app/leer_ticks_spark.py
```

**Resultado esperado:** aparece una tabla con la columna `mensaje` y cotizaciones JSON. En la computadora ejecutora se observaron **11 mensajes** acumulados; una computadora recién configurada puede mostrar **cinco** si antes se ejecutó una sola vez el productor.

**Qué significa lo principal:** `--network container:kafka-trading` permite al contenedor temporal llegar a Kafka, `-v ...` comparte con Spark los archivos `src/`, `--packages` añade el conector Kafka y `spark.jars.ivy=/tmp/spark-ivy` indica a Ivy una carpeta válida para sus dependencias. La configuración es provisional y se sustituirá por una red y persistencia más reproducibles en etapas posteriores.

**IMPORTANTE:** esta prueba **no crea velas OHLC**; únicamente lee los mensajes de `ticks`.

### A12. Verificación final de instalación

Marcar solo cuando realmente se vea el resultado:

- [ ] `python --version` muestra Python y se puede crear `.venv`.
- [ ] `git --version` responde y el repositorio se clonó con archivos `src/`.
- [ ] Docker Desktop está iniciado y `hello-world` muestra su mensaje.
- [ ] `docker ps` muestra el contenedor `kafka-trading` funcionando.
- [ ] Kafka lista `ticks`, `velas` y `signals`.
- [ ] `python src/productor_ticks.py` genera mensajes ficticios.
- [ ] Spark muestra mensajes JSON procedentes de `ticks`.

Si todos se cumplen, la nueva computadora **reproduce el avance del corte del 21/11/2026**, no el proyecto final completo.

## B. Continuar en la computadora que ya funciona

En la computadora de María ya se comprobaron Python 3.11.9, Docker, Kafka y Spark. **No reinstalar ni volver a crear estos componentes.**

1. Abrir Docker Desktop.
2. Abrir VS Code en `C:\Users\maria\Desktop\Proyecto_Trading_Algoritmico`.
3. Abrir PowerShell en la carpeta raíz y activar el entorno:

```powershell
.\.venv\Scripts\Activate.ps1
```

4. Si Kafka estaba detenido, iniciarlo; si ya está `Up`, no hacer nada:

```powershell
docker ps -a
```

5. Comprobar los tópicos y scripts:

```powershell
docker exec kafka-trading /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
Get-ChildItem .\src
```

6. **No clonar el repositorio dentro de la carpeta que ya contiene el proyecto.** Para subir este primer corte, seguir las instrucciones de Git en el README y revisar `git status` antes del commit.

## C. Errores frecuentes y cómo resolverlos

| Lo que aparece | Causa probable | Qué comprobar o hacer |
|---|---|---|
| `docker: command not found` | Docker no está instalado o no aparece en PATH. | Revisar instalación de Docker Desktop; abrir una terminal nueva. |
| `Cannot connect to the Docker daemon` o no aparece `Server` | Docker Desktop no ha iniciado. | Abrir Docker Desktop y esperar a que esté funcionando. |
| `WSL1 is not supported...` | WSL 1 no está habilitado. | No suele importar: el proyecto usa WSL 2. Confirmar `docker info`. |
| `unknown flag: --no` en Docker | Se escribió `--no stream` con espacio. | Usar `docker stats --no-stream kafka-trading`. |
| `Conflict. The container name ... is already in use` | `kafka-trading` ya existe. | Usar `docker ps -a`; si está detenido, `docker start kafka-trading`. No eliminarlo. |
| No aparece `(.venv)` | Entorno no activado o terminal nueva. | Ejecutar ` .\.venv\Scripts\Activate.ps1 ` desde la raíz, o invocar directamente ` .\.venv\Scripts\python.exe `. |
| PowerShell bloquea `Activate.ps1` | Política local de scripts. | Ejecutar directamente ` .\.venv\Scripts\python.exe `, sin modificar políticas del sistema. |
| `ModuleNotFoundError: confluent_kafka` | Falta instalar la librería en el entorno correcto. | Activar `.venv`; luego `python -m pip install -r requirements.txt`. |
| Spark: `FileNotFoundException /nonexistent/.ivy...` | Ivy intentó guardar dependencias en una carpeta no válida. | Usar el comando A11, que incorpora `-e HOME=/tmp` y `--conf spark.jars.ivy=/tmp/spark-ivy`. |
| Spark se queda sin memoria o contenedor termina | RAM insuficiente para procesos simultáneos. | Cerrar apps, comprobar `docker stats --no-stream` y los límites. No cambiar parámetros al azar. |
| Spark no muestra mensajes | Kafka vacío, productor no ejecutado, tópico incorrecto o conexión fallida. | Ejecutar A10, comprobar `ticks`, después volver a ejecutar A11. |
| `git clone` no contiene `src/*.py` | Aún no se subieron los archivos locales del ejecutor. | Completar la primera subida de código desde esa computadora. |

## D. Qué NO se instala ni se configura todavía

- **Interactive Brokers / TWS:** la cuenta paper fue creada, pero el acceso y la verificación aún estaban pendientes al cierre del 21/11/2026. No hay pasos comprobados de TWS/API para documentar como terminados.
- **Kafka en Docker Compose:** previsto para una arquitectura posterior; no se usó en las pruebas actuales.
- **Spark como servicio permanente o velas de un minuto:** pendiente del paso 15; por ahora se ejecuta un lector temporal.
- **Algoritmo de trading, señales y órdenes:** pendientes. No se manejan fondos ni órdenes reales.

## E. Fuentes de instalación oficiales

- [Python 3.11.9](https://www.python.org/downloads/release/python-3119/)
- [VS Code](https://code.visualstudio.com/download)
- [Git for Windows](https://git-scm.com/install/windows)
- [Docker Desktop para Windows](https://docs.docker.com/desktop/setup/install/windows-install/)
- [Apache Kafka 4.4: Docker](https://kafka.apache.org/44/getting-started/docker/)
- [Apache Spark 4.2.1: integración con Kafka](https://spark.apache.org/docs/4.2.1/structured-streaming-kafka-integration.html)
- [Spark 4.2.1: configuración de Ivy](https://spark.apache.org/docs/4.2.1/configuration.html)
