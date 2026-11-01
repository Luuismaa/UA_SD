# Imagen de WM_Central. Se construye DESDE LA RAÍZ del repo (necesita el pom padre y common):
#   docker build -f WM_Central/Dockerfile -t wm-central .

# ---- Etapa 1: compilar con Maven + JDK 17 (solo sirve para generar el jar) ----
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY . .
# -pl WM_Central -am: compila WM_Central y lo que necesita (common), nada más
RUN mvn -q -B -pl WM_Central -am package

# ---- Etapa 2: ejecutar con solo el JRE 17 (imagen final, más ligera) ----
# No Alpine: la librería nativa de SQLite necesita glibc
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/WM_Central/target/WM_Central.jar app.jar

# Carpeta de la BD. Se monta aquí un volumen Docker para que central.db sobreviva a reinicios y despliegues
RUN mkdir -p /data

# Parámetros por variables de entorno (nada fijo en el código):
#   PUERTO_SOCKETS  puerto donde escucha a los Monitors (hay que abrirlo en el Security Group de AWS)
#   KAFKA_BROKER    host:puerto de Kafka (obligatoria)
#   RUTA_DB         archivo SQLite
#   PUERTO_WEB      panel web de la Central (http://IP:PUERTO_WEB), también abierto en el Security Group
ENV PUERTO_SOCKETS=5050 \
    RUTA_DB=/data/central.db \
    PUERTO_WEB=8080
EXPOSE 5050 8080

# Sin pantalla en el contenedor: no hay ventana, la Central se maneja desde el panel web
CMD exec java -Djava.awt.headless=true -jar app.jar "$PUERTO_SOCKETS" "$KAFKA_BROKER" "$RUTA_DB" "$PUERTO_WEB"
