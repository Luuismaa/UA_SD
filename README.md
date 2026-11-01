# WaterManagement (WM) - Sistema Distribuido de Gestión de Riego Urbano

Práctica de Sistemas Distribuidos (curso 26/27). Simulación de una red de estaciones de riego de una ciudad, gestionada
desde una central y usada por operarios de campo. Combina **sockets TCP** (con tramas `<STX><DATA><ETX><LRC>`) y
**Apache Kafka** (streaming de eventos). La parte central se despliega en **AWS**; estaciones y operarios, en Docker en
los PCs del laboratorio.

---

## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Arquitectura del Sistema](#arquitectura-del-sistema)
- [Componentes](#componentes)
- [Tecnologías Utilizadas](#tecnologías-utilizadas)
- [Protocolos y Comunicaciones](#protocolos-y-comunicaciones)
- [Estructura del Repositorio](#estructura-del-repositorio)
- [Compilación y Ejecución](#compilación-y-ejecución)
- [Despliegue](#despliegue)
- [Autores](#autores)

---

## Descripción General

1. Los operarios de campo (`WM_FO`) solicitan riegos de forma manual o automatizada desde un fichero.
2. La central (`WM_Central`) valida la petición (operario registrado, estación disponible), ordena el riego y va
   notificando cada paso al operario.
3. La estación de riego (`WM_WS`) abre la electroválvula, envía caudal y volumen cada segundo y avisa de cualquier
   avería (fuga). El riego termina al cumplirse el tiempo, desde el menú de la estación o por orden de la central.
4. La central guarda estaciones, operarios y riegos en **SQLite** y muestra el estado de cada estación en un
   **panel web** en tiempo real, desde el que también puede iniciar riegos, bloquear y activar estaciones (a una o a todas).

Estados de una estación: **Disponible** (verde), **Regando** (verde parpadeante), **Fuga** (rojo),
**Fuera de Servicio** (naranja) y **Desconectada** (gris).

---

## Arquitectura del Sistema

```text
            PC LAB 1                         AWS (EC2 + Elastic IP)                     PC LAB 2
  +------------------+              +-------------------------------+         +------------------------+
  |  WM_FO (1..n)    |---- Kafka -->|  Kafka           :9092        |<- Kafka-|  WM_WS_E  (Engine)     |
  |  (operarios)     |              |  (Broker-Kafka)               |         |     ^  socket          |
  +------------------+              |                               |         |     |  SALUD / OK / KO   |
                                    |  WM_Central      :5050 sockets|<-socket-|  WM_WS_M  (Monitor)    |
     navegador ------- HTTP ------->|  + SQLite        :8080 panel  |         |  WM_WS (1..n)          |
                                    +-------------------------------+         +------------------------+
```

- **Sockets:** Monitor ↔ Central (alta, autenticación, latido, fugas) y Engine ↔ Monitor (salud cada segundo).
- **Kafka:** todo lo demás (peticiones, órdenes, pasos de autorización, telemetría, fin de riego, estado global).
- Ningún componente usa `sleep`: las tareas periódicas y los reintentos usan `ScheduledExecutorService`.

---

## Componentes

### 1. WM_Central (Sistema Central)
- **Servidor de sockets concurrente** (un hilo por Monitor): autenticación (`AUTH`), latido cada segundo y avisos de
  fuga. Si deja de recibir latidos durante 3 s, marca la estación como Desconectada.
- **Kafka:** procesa las solicitudes de riego (valida operario y estado de la estación), ordena `INICIAR`, `PARAR`,
  `BLOQUEAR` y `ACTIVAR`, reenvía la telemetría al operario y le envía los pasos, el resumen y los avisos de fuga.
  Publica el estado de todas las estaciones en cada cambio y cada 5 s.
- **SQLite:** estaciones (con su ubicación y bloqueo), operarios y riegos. Al arrancar muestra todas las estaciones
  registradas como Desconectadas hasta que su Monitor se autentica.
- **Panel web** (`http://IP:8080`): tabla con colores, caudal, volumen y operario, registro de eventos y acciones del
  punto 11 a una o a todas las estaciones. En un PC con pantalla abre además una ventana de escritorio (Swing).
- **Resiliencia:** cierra los riegos huérfanos (si cae el Monitor o el Engine), sigue funcionando sin Kafka y, tras
  reiniciarse, recupera de la BD los riegos en curso.

### 2. WM_WS (Watering Station / Estación de Riego)
- **WM_WS_M (Monitor):** se autentica en la Central con su ID y ubicación, envía un latido cada segundo y reconecta
  solo si la Central cae. Ofrece un servidor de sockets a su Engine, le asigna su ID y le pide salud cada segundo; si
  el Engine responde `KO` o no responde, avisa de la fuga a la Central.
- **WM_WS_E (Engine):** simula la electroválvula y el caudalímetro. Recibe órdenes por Kafka y envía telemetría cada
  segundo. Menú: simular avería (KO), riego manual y parada manual. Guarda el riego en curso en un archivo local para
  enviar lo regado si se cae y se recupera. Sin Monitor, se declara averiada y no admite riegos.

### 3. WM_FO (Field Operator / Operario de Campo)
- Interfaz gráfica (Swing): lista de estaciones con su estado, solicitud de riego, pasos de autorización, telemetría en
  tiempo real, resumen final y avisos de fuga.
- **Modo fichero:** lanza las peticiones de un archivo (`WS-03#60` por línea), cada una 4 s después de terminar la anterior.
- Muestra *"Imposible conectar con WM_Central"* si deja de recibir el estado global durante 10 s.

---

## Tecnologías Utilizadas

- **Lenguaje:** Java 17, proyecto **Maven multimódulo** (`common`, `WM_Central`, `WM_WS`, `WM_FO`).
- **Mensajería:** Apache Kafka en modo KRaft (librería `kafka-clients` 3.7).
- **Comunicación punto a punto:** sockets TCP con tramas `<STX><DATA><ETX><LRC>` y diálogo `ENQ/ACK/NACK/EOT`.
- **Base de datos:** SQLite (`sqlite-jdbc`).
- **Contenedores:** Docker y Docker Compose (imágenes de dos etapas: compilación con Maven y ejecución con JRE 17).
- **Cloud:** AWS Academy Learner Lab — dos instancias EC2 (Kafka y Central) con Elastic IP.
- **CI/CD:** VS Code → GitHub → GitHub Actions → Docker Hub (`luuiss/wm-central`, `luuiss/wm-ws`, `luuiss/wm-fo`) → AWS.

---

## Protocolos y Comunicaciones

Todos los mensajes son texto UTF-8 con el formato `CODIGO#campo1#campo2#...`. IDs: estaciones `WS-01`, operarios `FO-01`.

### 1. Trama de sockets

```text
<STX><DATA><ETX><LRC>
```
- `STX` (`0x02`) inicio · `DATA` el mensaje · `ETX` (`0x03`) fin · `LRC` = XOR byte a byte de `DATA` y `ETX`.
- Diálogo: el cliente abre con `ENQ` (`0x05`) y el servidor responde `ACK` (`0x06`). Cada trama se confirma con `ACK`
  o con `NACK` (`0x15`, LRC incorrecto: se reenvía hasta 3 veces). La despedida es `EOT` (`0x04`).
- Implementado una sola vez en `common` (`SocketProtocol`, `ConexionesTramas`) y usado por Central, Monitor y Engine.

**Monitor ↔ Central** (conexión persistente mientras la estación está viva):

| Mensaje | Sentido | Significado |
|---|---|---|
| `AUTH#WS-03#Parque Central` | Monitor → Central | Alta y autenticación con ID y ubicación |
| `AUTH_OK#WS-03` | Central → Monitor | Aceptada |
| `AUTH_KO#WS-03#motivo` | Central → Monitor | Rechazada: `NO_REGISTRADA`, `ID_DUPLICADO` o `FORMATO_INVALIDO` |
| `LATIDO#WS-03` | Monitor → Central | Cada segundo (3 s sin mensajes = Desconectada) |
| `FUGA#WS-03` / `FUGA_RESUELTA#WS-03` | Monitor → Central | Avería detectada / resuelta |

**Engine ↔ Monitor:** `IDWS#WS-03` (el Monitor asigna el ID al Engine), `SALUD` cada segundo y respuesta `OK` o `KO`.

### 2. Topics de Kafka

10 topics (1 partición, factor de replicación 1), creados por la Central al arrancar.

| Topic | Sentido | Clave | Mensaje |
|---|---|---|---|
| `solicitarRiego` | FO / WS / Central → Central | idWS | `SOLICITUD#FO-12#WS-03#60` (solicitante FO-xx, `MANUAL` o `CENTRAL`) |
| `ordenesRiego` | Central → WS | idWS | `INICIAR#WS-03#FO-12#60` · `PARAR#WS-03#motivo` · `BLOQUEAR#WS-03` · `ACTIVAR#WS-03` |
| `riegoPasosAutorizacion` | Central → FO | idFO | `PASO#FO-12#WS-03#paso#detalle` (`RECIBIDA`, `VALIDANDO`, `ESPERANDO_WS`, `AUTORIZADA`, `DENEGADA`) |
| `inicioRiego` | WS → Central | idWS | `INICIO#WS-03#FO-12#2026-10-20T12:04:32` |
| `estadoRiegoWS` | WS → Central | idWS | `TELEMETRIA#WS-03#12.4#186.0` (caudal L/min, volumen L), cada segundo |
| `estadoRiegoFO` | Central → FO | idFO | `TELEMETRIA#FO-12#WS-03#12.4#186.0` |
| `finRiego` | WS → Central | idWS | `FIN#WS-03#motivo#620.0#60` (motivo `TIEMPO`, `MANUAL`, `FUGA`, `BLOQUEO`, `CENTRAL`) |
| `resumenRiego` | Central → FO | idFO | `RESUMEN#FO-12#WS-03#620.0#60#TIEMPO` |
| `fugaRiego` | Central → FO | idFO | `FUGA#FO-12#WS-03#240.5` |
| `estadoGlobalWS` | Central → todos los FO | idWS | `ESTADO_WS#WS-03#Parque Central#DISPONIBLE`, en cada cambio y cada 5 s |

El protocolo completo, con los flujos de cada escenario, está en el documento *WaterManagement — Protocolo de comunicación*.

---

## Estructura del Repositorio

```text
WaterManagement/
├── pom.xml                  POM padre (módulos y versiones)
├── common/                  Código compartido: tramas, sockets, topics, estados
├── WM_Central/              Central: sockets, Kafka, SQLite, panel web (Dockerfile)
├── WM_WS/                   Estación: Monitor y Engine (Dockerfile)
├── WM_FO/                   Operario de campo (Dockerfile)
├── compose.yml              Kafka local para desarrollo
├── compose-ws.yml           Lanzar una estación (Monitor + Engine) con Docker
├── compose-fo.yml           Lanzar un operario con Docker (X11)
├── .github/workflows/       CI/CD: construcción y publicación de imágenes en Docker Hub
├── docs/                    Guía de despliegue y resúmenes de desarrollo
├── prompts/                 Prompts usados con asistentes de IA
├── Nombres.txt
└── README.md
```

---

## Compilación y Ejecución

Compilar siempre desde la raíz (requiere Java 17+ y Maven):

```bash
mvn clean package
```

| Módulo | Ejecutable | Argumentos |
|---|---|---|
| Central | `WM_Central/target/WM_Central.jar` | `<puerto sockets> <IP:puerto Kafka> <ruta BD> [puerto panel web, 8080 por defecto]` |
| Monitor | `WM_WS/target/WM_WS_M.jar` | `<puerto para el Engine> <IP:puerto Central> <ID WS> "<ubicación>"` |
| Engine | `WM_WS/target/WM_WS_E.jar` | `<IP:puerto Kafka> <IP:puerto Monitor>` |
| Operario | `WM_FO/target/WM_FO.jar` | `<IP:puerto Kafka> <ID FO>` |

Ejemplo en local (con el Kafka de `compose.yml`: `docker compose up -d`):

```bash
java -jar WM_Central/target/WM_Central.jar 5050 localhost:9092 central.db
java -jar WM_WS/target/WM_WS_M.jar 6000 localhost:5050 WS-01 "Parque Central"
java -jar WM_WS/target/WM_WS_E.jar localhost:9092 localhost:6000
java -jar WM_FO/target/WM_FO.jar localhost:9092 FO-01
```

La primera vez, la Central crea la BD con las estaciones `WS-01` a `WS-20` y los operarios `FO-01` a `FO-10`.

---

## Despliegue

- **Cloud (AWS):** Kafka en `18.211.92.44:9092` y la Central en `184.194.154.93` (sockets `:5050`, panel web
  **http://184.194.154.93:8080**), ambos en Docker con reinicio automático.
- **Laboratorio:** estaciones con `compose-ws.yml` y operarios con `compose-fo.yml`, usando las imágenes de Docker Hub
  (no hace falta compilar).

Paso a paso, guion de demostración y solución de problemas: **[docs/GUIA_DESPLIEGUE.md](docs/GUIA_DESPLIEGUE.md)**.

---

## Autores

- **Luis Manuel Redondo Marín:** `WM_Central` (servidor de sockets, registro de estaciones, flujo de riego por Kafka,
  SQLite, panel web), tramas y sockets en `common` (`ConexionesTramas`, `SocketClient`, `SocketServer`), Docker de la
  Central, despliegue en AWS y CI/CD.
- **Jaime Sarrias Hernández:** `WM_WS_M` y `WM_WS_E` (salud, riego, telemetría, menú y persistencia), `WM_FO`
  (interfaz y Kafka), `SocketProtocol`, interfaz de escritorio de la Central, Dockerfiles y compose de WS y FO.
