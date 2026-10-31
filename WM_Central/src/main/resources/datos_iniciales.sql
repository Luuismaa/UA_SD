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

INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-01', 'Ana García');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-02', 'Carlos López');
INSERT OR IGNORE INTO operario (id, nombre) VALUES ('FO-03', 'Lucía Martín');
