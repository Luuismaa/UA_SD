-- Datos iniciales de la BD de WM_Central.
-- Solo se ejecuta la PRIMERA vez que arranca la Central (cuando la tabla estacion está vacía).
-- Para añadir estaciones u operarios después, sin recompilar:
--   sqlite3 central.db "INSERT INTO estacion(id, ubicacion) VALUES ('WS-07', 'Parque Sur');"
-- Formato: una sentencia por bloque terminada en ';' (no usar ';' dentro de los textos).

INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-01', 'Parque Central');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-02', 'Jardín Norte');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-03', 'Paseo del Río');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-04', 'Parque del Castillo');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-05', 'Jardín Botánico');
-- Margen para la corrección: el profesor puede pedir lanzar tantas WS como quiera
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-06', 'Parque Sur');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-07', 'Parque Sur');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-08', 'Glorieta Norte');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-09', 'Glorieta Norte');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-10', 'Parque del Lago');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-11', 'Parque del Lago');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-12', 'Jardines del Palacio');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-13', 'Jardines del Palacio');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-14', 'Huerto Urbano');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-15', 'Plaza Mayor');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-16', 'Paseo Marítimo');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-17', 'Paseo Marítimo');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-18', 'Campus Universitario');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-19', 'Campus Universitario');
INSERT OR IGNORE INTO estacion (id, ubicacion) VALUES ('WS-20', 'Parque Infantil');

INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-01', 'Ana García');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-02', 'Carlos López');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-03', 'Lucía Martín');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-04', 'Javier Ruiz');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-05', 'Marta Sánchez');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-06', 'Pablo Gómez');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-07', 'Elena Torres');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-08', 'Diego Navarro');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-09', 'Sara Romero');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-10', 'Hugo Molina');
