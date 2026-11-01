# WaterManagement — Guía de despliegue para la corrección

Escenario de la corrección (Figura 4 del enunciado):

| Dónde | Qué | Dirección |
|---|---|---|
| **AWS** (EC2 `Broker-Kafka`) | Kafka | `18.211.92.44:9092` |
| **AWS** (EC2 `WM-Central`) | WM_Central + SQLite | sockets `184.194.154.93:5050` · panel web **http://184.194.154.93:8080** |
| **PC LAB 1** | WM_FO (1) y WM_FO (2) | se conectan a Kafka |
| **PC LAB 2** | WM_WS (1) y WM_WS (2) = Monitor + Engine | Monitor → Central (sockets) · Engine → Kafka |

```
 PC LAB 1                          AWS                              PC LAB 2
 WM_FO ──────── Kafka 18.211.92.44:9092 ◄───── Engine (WM_WS_E)
 WM_FO ────────┘        ▲                       │ socket (SALUD)
                        │                       ▼
                WM_Central 184.194.154.93 ◄── Monitor (WM_WS_M)
                 :5050 sockets  :8080 panel web
```

Las IP son **Elastic IP**: no cambian aunque el Learner Lab apague y encienda las máquinas.

---

## 0. El día antes

1. Todo el código bueno en **`master`** y push → en GitHub, pestaña **Actions**, los tres trabajos en verde
   (imágenes `luuiss/wm-central`, `luuiss/wm-ws`, `luuiss/wm-fo` en Docker Hub).
2. Actualizar la Central de AWS con la última imagen (sección 5).
3. Ensayar el escenario completo (secciones 1 a 4) y el guion de la sección 6.
4. En cada PC del laboratorio que se vaya a usar: tener **Docker** instalado y descargar las imágenes de antemano
   (`docker pull luuiss/wm-ws:latest` y `docker pull luuiss/wm-fo:latest`).

---

## 1. Parte cloud (AWS) — llega ya desplegada

1. **AWS Academy → Learner Lab → `Start Lab`**. Esperar a que el punto junto a "AWS" esté **verde**
   (la sesión dura ~4 h; al caducar las máquinas se paran y se vuelven a encender con `Start Lab`).
2. Pulsar **AWS** → consola → **EC2 → Instancias**: `Broker-Kafka` y `WM-Central` en **Running**.
3. Comprobar Kafka: `Broker-Kafka` → **Conectar → EC2 Instance Connect**:
   ```bash
   sudo docker ps              # el contenedor "kafka" debe estar "Up"
   ```
4. Comprobar la Central: `WM-Central` → **Conectar → EC2 Instance Connect**:
   ```bash
   sudo docker ps              # el contenedor "central" debe estar "Up"
   sudo docker logs -f central # ver la Central en directo (Ctrl+C para salir, la Central sigue)
   ```
   Tienen `--restart unless-stopped`: arrancan solos al encenderse la máquina.
5. Abrir el **panel web** de la Central en el navegador: **http://184.194.154.93:8080**
   (estado de todas las WS con sus colores, registro de eventos y botones regar / bloquear / activar, a una o a todas).

> **Antes de lanzar nada en el laboratorio, la parte cloud tiene que estar así.**

---

## 2. PC LAB 2 — Estaciones de riego (WM_WS = Monitor + Engine) con Docker

Solo hace falta el archivo `compose-ws.yml` del repo (clonar el repo o copiar ese archivo). Ya apunta a AWS por defecto.

**Estación WS-01** (terminal en la carpeta donde esté `compose-ws.yml`):
```bash
ID_WS=WS-01 UBICACION="Parque Central" docker compose -f compose-ws.yml -p ws1 up -d
```
**Estación WS-02:**
```bash
ID_WS=WS-02 UBICACION="Jardín Norte" docker compose -f compose-ws.yml -p ws2 up -d
```
- `-p ws1`, `-p ws2`… es el nombre de cada estación: se pueden lanzar tantas como pida el profesor
  (cambiando `-p` e `ID_WS`). Hay **WS-01 a WS-20** dadas de alta en la BD.
- En el panel web, cada estación pasa de **gris (Desconectada)** a **verde (Disponible)**.

**Ver lo que hace el Monitor** (se autentica, salud del Engine, fugas):
```bash
docker compose -p ws1 logs -f monitor
```
**Usar el menú del Engine** (1 = simular avería/KO, 2 = riego manual, 3 = parar riego manual):
```bash
docker attach ws1-engine-1
```
> Para **salir del attach sin parar el Engine: `Ctrl+P` y después `Ctrl+Q`** (Ctrl+C lo pararía).

**Parar una estación:**
```bash
docker compose -p ws1 down
```

Si la Central o Kafka estuvieran en otra IP, se indican así: `CENTRAL_IP=x.x.x.x KAFKA_IP=y.y.y.y ID_WS=... docker compose ...`

---

## 3. PC LAB 1 — Operarios (WM_FO) con Docker

El FO tiene ventana (Swing), así que el contenedor necesita la pantalla del PC (X11).

**Linux** (una vez por sesión):
```bash
xhost +local:docker
```
**Operario FO-01:**
```bash
docker run -it --rm -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix \
  luuiss/wm-fo:latest 18.211.92.44:9092 FO-01
```
**Operario FO-02** (otra terminal): igual, cambiando `FO-01` por `FO-02`. Hay **FO-01 a FO-10** dados de alta.

- macOS: instalar XQuartz (Preferencias → Seguridad → "Allow connections from network clients"), reiniciar sesión,
  `xhost + 127.0.0.1` y usar `-e DISPLAY=host.docker.internal:0` (sin el `-v`).
- Windows: VcXsrv con "Disable access control" y `-e DISPLAY=host.docker.internal:0.0` (sin el `-v`).

En la ventana del FO: a los ~5 s aparecen las estaciones en la tabla; elegir una, la duración y **Solicitar Riego**.
**Ejecutar Fichero…** lanza las peticiones de un archivo (una línea por riego: `WS-01#30`), esperando 4 s tras acabar cada una.

---

## 4. Plan B — sin Docker, con los ejecutables (.jar)

Solo si Docker fallase en un PC. Hace falta **Java 17 o superior**. Los jar se generan en cualquier portátil con
`mvn clean package` (desde la raíz del repo) y se copian:

| Ejecutable | Ruta tras compilar |
|---|---|
| Monitor | `WM_WS/target/WM_WS_M.jar` |
| Engine | `WM_WS/target/WM_WS_E.jar` |
| Operario | `WM_FO/target/WM_FO.jar` |

**Estación WS-01** (dos terminales):
```bash
java -jar WM_WS_M.jar 6000 184.194.154.93:5050 WS-01 "Parque Central"   # Monitor: <puerto para el Engine> <Central> <ID> <ubicación>
java -jar WM_WS_E.jar 18.211.92.44:9092 localhost:6000                  # Engine:  <Kafka> <Monitor>
```
Para otra estación en el mismo PC, otro puerto: `6001 … WS-02 "Jardín Norte"` y el Engine con `localhost:6001`.

**Operario FO-01:**
```bash
java -jar WM_FO.jar 18.211.92.44:9092 FO-01
```

---

## 5. Actualizar la Central de AWS (nueva versión del código)

1. Merge a `master` y push → esperar el verde en **Actions**.
2. En `WM-Central` (EC2 Instance Connect):
   ```bash
   sudo docker pull luuiss/wm-central:latest
   sudo docker rm -f central
   sudo docker run -d --name central --restart unless-stopped -p 5050:5050 -p 8080:8080 \
     -e KAFKA_BROKER=18.211.92.44:9092 -v central-datos:/data luuiss/wm-central:latest
   ```
   La BD no se pierde: está en el volumen `central-datos`. **Kafka no hay que tocarlo nunca.**

---

## 6. Guion de demostración (guía de corrección)

### Funcionamiento base

| Qué enseñar | Cómo | Qué se ve |
|---|---|---|
| La Central muestra las WS de la BD como Desconectadas | Abrir el panel antes de lanzar ninguna WS | 20 WS en gris |
| Alta y autenticación de una WS | Lanzar WS-01 (sección 2) | Panel: WS-01 verde. Log: `WS-01 autenticada` |
| Denegación de una WS no registrada | `ID_WS=WS-99 ... -p ws99 up` | El Monitor termina con `NO_REGISTRADA` |
| Riego pedido por un FO | En el FO: WS-01, 20 s, Solicitar | FO: pasos hasta AUTORIZADA, caudal/volumen, resumen. Panel: verde parpadeante con caudal, volumen y operario |
| Riego denegado | Pedir riego en una WS gris o naranja | FO: `DENEGADA` con el motivo |
| Riego manual desde la WS | Menú del Engine → `2` | Panel: regando con operario `MANUAL` |
| Iniciar / bloquear / activar desde la Central, a una o a todas | Botones del panel web | Bloquear: naranja "Fuera de Servicio" (y el riego en curso se corta). Activar: verde |
| Fuga | Menú del Engine → `1` (otra vez `1` para resolverla) | Panel: rojo. Si regaba: el riego se corta y el FO recibe el aviso de fuga y el resumen. Al resolverla: verde |
| Modo fichero del FO | Ejecutar Fichero… | Un riego tras otro, 4 s después de acabar el anterior |

### Resiliencia (parar módulos de golpe con Ctrl+C o cerrando la terminal)

| Caso | Cómo provocarlo | Comportamiento esperado |
|---|---|---|
| Cae un **Monitor** | `docker compose -p ws1 kill monitor` | Central: WS **Desconectada** (gris) a los ≤3 s. Si regaba, el riego se cierra (motivo CENTRAL) y el FO recibe el resumen. El Engine deja de aceptar riegos |
| Cae un **Engine** | `docker compose -p ws1 kill engine` | El Monitor avisa de avería → WS en **rojo**. Si regaba, el FO recibe el aviso. Al volver (`docker compose -p ws1 up -d`) la fuga se resuelve, el riego pendiente se cierra con lo regado y el FO recibe su resumen |
| Cae un **FO** durante un riego | Cerrar la ventana del FO | El riego sigue. Al volver a abrir el FO con el mismo ID, recibe los mensajes que se perdió (resumen) |
| Cae la **Central** | En `WM-Central`: `sudo docker restart central` (o `kill`) | Las WS siguen regando hasta terminar. Monitors y FO muestran "Imposible conectar con WM_Central" y reconectan solos. Al volver, la Central recupera los riegos en curso de la BD y envía los resúmenes |
| Nuevas instancias | Lanzar más WS (`-p ws3`, `ID_WS=WS-03`) o más FO | Aparecen sin reiniciar nada |

---

## 7. Problemas frecuentes

| Síntoma | Causa / solución |
|---|---|
| Nada responde en AWS | El Learner Lab no está iniciado: `Start Lab` y esperar a que las EC2 estén Running |
| `kafka` en estado `Restarting` | Falta de memoria: recrearlo con la orden de la sección 8 (lleva `KAFKA_HEAP_OPTS=256m`) |
| El panel web no carga | Comprobar que `central` está Up y que el puerto 8080 está abierto en el grupo de seguridad `default` |
| El Monitor dice `NO_REGISTRADA` | Ese ID no está en la BD: usar WS-01 a WS-20 |
| El Monitor dice `ID_DUPLICADO` | Ya hay una estación con ese ID conectada (o la anterior acaba de caerse: reintenta sola en 3 s) |
| El FO no muestra estaciones | Esperar 5 s; comprobar la IP de Kafka en el comando |
| FO en Docker: `cannot open display` | Falta `xhost +local:docker` (Linux) o el servidor X (XQuartz / VcXsrv) |
| Riego "La estación no ha respondido" justo tras reiniciar un Engine | Kafka tarda unos segundos en reasignar el consumidor del Engine: esperar y repetir |

---

## 8. Montaje desde cero de la parte cloud (referencia)

Solo si hubiera que volver a crear las máquinas. Ambas: **Ubuntu, t2.micro, sin par de claves, grupo de seguridad
`default`** con entradas abiertas (0.0.0.0/0) en **22, 9092, 5050 y 8080**, y una **Elastic IP** asociada a cada una.

En cada EC2 (Docker + 1 GB de swap, porque la t2.micro solo tiene 1 GB de RAM):
```bash
sudo apt update -y && sudo apt install -y docker.io && sudo systemctl enable --now docker
sudo fallocate -l 1G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile && echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

**Kafka** (en `Broker-Kafka`; `ADVERTISED_LISTENERS` debe llevar SU Elastic IP):
```bash
sudo docker run -d --name kafka --restart unless-stopped -p 9092:9092 \
  -e KAFKA_HEAP_OPTS="-Xmx256m -Xms256m" \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://18.211.92.44:9092 \
  bitnamilegacy/kafka:3.7.0
```

**Central** (en `WM-Central`): la orden de la sección 5. La primera vez crea la BD con WS-01..WS-20 y FO-01..FO-10.

**CI/CD**: `.github/workflows/deploy.yml` construye las tres imágenes en cada push a `master` (secrets del repo
`DOCKER_USERNAME` y `DOCKER_PASSWORD`).
