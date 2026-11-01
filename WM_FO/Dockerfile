# Etapa 1: Compilación
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /src
COPY . .
# -DskipTests agiliza la compilación en el laboratorio
RUN mvn -q -B -pl WM_FO -am package -DskipTests

# Etapa 2: Ejecución
FROM eclipse-temurin:17-jre
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxext6 libxrender1 libxtst6 libxi6 fontconfig && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Usa comodín *.jar para evitar fallos si el nombre es WM_FO-1.0.jar en vez de WM_FO.jar
COPY --from=builder /src/WM_FO/target/*.jar ./app.jar

# Variables de entorno con las IPs de AWS por defecto
ENV KAFKA_IP="18.211.92.44:9092"
ENV FO_ID="FO-01"

# Permite inyectar argumentos de Kafka al arrancar
CMD ["sh", "-c", "java -jar app.jar ${KAFKA_IP} ${FO_ID}"]