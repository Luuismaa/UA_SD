# Etapa 1: Compilación
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /src
COPY . .
# Compila solo el módulo WS y sus dependencias (common)
RUN mvn -q -B -pl WM_WS -am package

# Etapa 2: Ejecución
FROM eclipse-temurin:17-jre
WORKDIR /app
# Copiamos solo los fat-jars específicos requeridos
COPY --from=builder /src/WM_WS/target/WM_WS_E.jar ./engine.jar
COPY --from=builder /src/WM_WS/target/WM_WS_M.jar ./monitor.jar
# No ponemos ENTRYPOINT fijo porque el docker-compose decidirá qué jar arrancar