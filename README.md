# WaterManagement (WM) - Sistema Distribuido de Gestión de Riego Urbano

Sistema distribuido en tiempo real desarrollado para la monitorización y automatización del riego urbano en parques y jardines. La arquitectura combina comunicación por Sockets TCP y Apache Kafka para la gestión de eventos, permitiendo un control centralizado de las estaciones de riego y el procesamiento de peticiones enviadas por operarios de campo.

---

## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Arquitectura del Sistema](#arquitectura-del-sistema)
- [Componentes](#componentes)
- [Tecnologías Utilizadas](#tecnologías-utilizadas)
- [Protocolos y Comunicaciones](#protocolos-y-comunicaciones)
- [Requisitos Previos](#requisitos-previos)
- [Autores](#autores)

---

## Descripción General

El proyecto simula el ciclo de vida completo de un servicio de riego urbano inteligente:
1. Los operarios de campo (`WM_FO`) solicitan servicios de riego de forma manual o automatizada mediante archivos.
2. El sistema central (`WM_Central`) valida la petición en función del estado de la estación objetivo y autoriza la apertura de caudal.
3. Las estaciones de riego (`WM_WS`) ejecutan la orden, registran las métricas de agua consumida cada segundo y notifican cualquier anomalía o fuga detectada.
4. La central mantiene un registro persistente en una base de datos SQLite y muestra el estado actualizado de cada estación en un panel visual en tiempo real.

---

## Arquitectura del Sistema

```text
+------------------+                        +------------------+
|      WM_FO       | -------> Kafka <------ |    WM_Central    |---------+
| (Field Operator) |                        |  (Core & Panel)  |          |
+------------------+                        +------------------+          |
                                                     |                    |
                                                     |                    |
                                                     v                    |
                                                   Kafka                  | Socket
                                                     ^                    |
                                                     |                    |
                                                     |                    |
                                            +------------------+          |
                                            |      WM_WS       |          |
                                            | (Engine & Mon.)  |----------+
                                            +------------------+
```

---

## Componentes

### 1. WM_Central (Sistema Central)
- Servidor Sockets TCP para la autenticación y monitorización continua del estado de salud de las estaciones.
- Procesamiento de eventos Kafka para coordinar solicitudes de riego y telemetría entrante.
- Base de datos SQLite para la persistencia del estado de la red, catálogo de estaciones y usuarios/operarios.
- Panel de control visual en tiempo real que refleja estados: *Disponible*, *Regando*, *Fuga*, *Fuera de servicio* y *Desconectada*.

### 2. WM_WS (Watering Station / Estación de Riego)
Cada estación se subdivide en dos módulos:
- **WM_WS_M (Monitor):** Se conecta a `WM_Central` mediante Sockets para autenticación inicial. Realiza comprobaciones periódicas de salud (ping/pong) con el motor local y reporta incidencias o fugas a la central.
- **WM_WS_E (Engine):** Simula el comportamiento de la electroválvula y el caudalímetro. Recibe las autorizaciones vía Kafka, mide el caudal (L/min) y volumen (L), y publica datos de telemetría cada segundo.

### 3. WM_FO (Field Operator / Operario de Campo)
- Aplicación de interfaz para que los operarios en campo soliciten órdenes de riego vía Kafka.
- Admite peticiones interactivas directas y lectura automatizada desde ficheros con un intervalo programado de 4 segundos entre solicitudes.

---

## Tecnologías Utilizadas

- **Lenguaje:** Java
- **Mensajería Event-Driven:** Apache Kafka (modo KRaft)
- **Comunicación Punto a Punto:** Sockets TCP
- **Base de Datos:** SQLite
- **Contenedores y Orquestación:** Docker y Docker Compose
- **Plataforma Cloud:** Railway (para el despliegue del broker de Kafka y `WM_Central` con integración CI/CD conectada a GitHub)

---

## Protocolos y Comunicaciones

### 1. Trama de Sockets (Central <-> Monitor)
La comunicación socket entre la Central y el Monitor de la estación sigue la estructura de trama normalizada:
```text
<STX><DATA><ETX><LRC>
```
- `STX`: Byte de inicio de trama (`\x02`).
- `DATA`: Carga útil estructurada del mensaje.
- `ETX`: Byte de fin de trama (`\x03`).
- `LRC`: Checksum de control de errores longitudinal (*Longitudinal Redundancy Check*).

### 2. Topics de Kafka
- `solicitudes_riego`: Peticiones enviadas por los operarios (`WM_FO`) hacia la central (`WM_Central`).
- `ordenes_estacion`: Comandos de inicio/parada enviados desde la central (`WM_Central`) hacia los motores (`WM_WS_E`).
- `telemetria_estaciones`: Muestras periódicas de caudal y consumo enviadas desde el motor (`WM_WS_E`) hacia la central (`WM_Central`).

---

## Requisitos Previos

- Docker Engine 20.10+
- Docker Compose 2.0+
- Java

---

## Autores

- **Luis:** Desarrollo de `WM_Central`, base de datos SQLite, `WM_FO`, infraestructura Cloud en Railway y CI/CD.
- **Jaime:** Desarrollo de `WM_WS_M`, `WM_WS_E`, protocolo de trama de Sockets y contenedorización local con Docker.
