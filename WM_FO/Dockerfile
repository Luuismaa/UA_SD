# Etapa 1: Compilación
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /src
COPY . .
RUN mvn -q -B -pl WM_FO -am package

# Etapa 2: Ejecución
FROM eclipse-temurin:17-jre
# Instalación de librerías gráficas para X11 sin headless
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxext6 libxrender1 libxtst6 libxi6 fontconfig && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /src/WM_FO/target/WM_FO.jar ./app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]