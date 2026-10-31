package wm.central;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Acceso a la base de datos SQLite de la Central. Todo el SQL del proyecto vive aquí.
 *
 * SQLite no es un servidor: es un archivo (rutaDB) que esta clase lee y escribe con JDBC.
 * Si el archivo no existe, SQLite lo crea al conectar.
 *
 * Concurrencia: SQLite solo admite un escritor a la vez y la Central tiene varios hilos
 * (un manejador por Monitor, más adelante Kafka y el menú). Por eso hay UNA conexión
 * compartida y todos los métodos son synchronized: las operaciones van de una en una.
 *
 * Seguridad: siempre PreparedStatement con '?'. Datos como la ubicación llegan por la red;
 * concatenarlos en el SQL permitiría inyección SQL.
 */
public class BaseDatos {

    /**
     * Datos de una estación tal y como están guardados en la BD.
     */
    public record DatosEstacion(String id, String ubicacion, boolean fueraServicio) { }

    private final String url;
    private Connection con;

    /**
     * @param rutaDB ruta del archivo .db (p. ej. "central.db" en local, "/data/central.db" en Docker)
     */
    public BaseDatos(String rutaDB) {
        this.url = "jdbc:sqlite:" + rutaDB;
    }

    /**
     * Abre la conexión, crea las tablas que falten y, si la BD está recién creada (sin
     * estaciones), la rellena con datos_iniciales.sql.
     * @throws SQLException si no se puede abrir o preparar la BD (la Central no puede arrancar sin ella)
     */
    public synchronized void inicializar() throws SQLException {
        con = DriverManager.getConnection(url);
        try (Statement st = con.createStatement()) {
            //En SQLite las claves foráneas (REFERENCES) solo se comprueban si se activan
            st.execute("PRAGMA foreign_keys = ON");

            st.executeUpdate(
                "CREATE TABLE IF NOT EXISTS estacion ("
              + "  id             TEXT PRIMARY KEY,"
              + "  ubicacion      TEXT NOT NULL,"
              + "  fuera_servicio INTEGER NOT NULL DEFAULT 0"   //1 = bloqueada por la Central
              + ")");
            st.executeUpdate(
                "CREATE TABLE IF NOT EXISTS operario ("
              + "  id     TEXT PRIMARY KEY,"
              + "  nombre TEXT"
              + ")");
            st.executeUpdate(
                "CREATE TABLE IF NOT EXISTS riego ("
              + "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
              + "  id_ws       TEXT NOT NULL REFERENCES estacion(id),"
              + "  solicitante TEXT NOT NULL,"   //FO-12, MANUAL o CENTRAL
              + "  inicio      TEXT NOT NULL,"   //ISO-8601, como en el protocolo
              + "  fin         TEXT,"            //NULL mientras el riego está en curso
              + "  duracion_s  INTEGER,"
              + "  volumen     REAL,"
              + "  motivo      TEXT"             //TIEMPO, MANUAL, FUGA, BLOQUEO, CENTRAL
              + ")");

            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM estacion")) {
                if (rs.next() && rs.getInt(1) == 0) {
                    System.out.println("[BD] Base de datos nueva: cargando datos_iniciales.sql");
                    ejecutarScript("/datos_iniciales.sql");
                }
            }
        }
        System.out.println("[BD] Base de datos lista (" + url + ")");
    }

    /**
     * Ejecuta un script SQL que va dentro del jar (carpeta resources).
     * Todo el script va en una transacción: o se aplica entero o no se aplica nada.
     */
    private void ejecutarScript(String recurso) throws SQLException {
        String sql;
        try (InputStream in = BaseDatos.class.getResourceAsStream(recurso)) {
            if (in == null) {
                throw new SQLException("No se encuentra el script " + recurso + " dentro del jar");
            }
            //Quitamos las líneas de comentario (--) y nos quedamos con el resto
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8).lines()
                    .filter(linea -> !linea.trim().startsWith("--"))
                    .collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new SQLException("No se puede leer " + recurso + ": " + e.getMessage(), e);
        }

        con.setAutoCommit(false);
        try (Statement st = con.createStatement()) {
            for (String sentencia : sql.split(";")) {
                if (!sentencia.isBlank()) {
                    st.executeUpdate(sentencia);
                }
            }
            con.commit();
        } catch (SQLException e) {
            con.rollback();
            throw e;
        } finally {
            con.setAutoCommit(true);
        }
    }

    /**
     * Todas las estaciones registradas (se cargan en el RegistroEstaciones al arrancar).
     */
    public synchronized List<DatosEstacion> cargarEstaciones() throws SQLException {
        List<DatosEstacion> lista = new ArrayList<>();
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT id, ubicacion, fuera_servicio FROM estacion ORDER BY id");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                lista.add(new DatosEstacion(rs.getString("id"), rs.getString("ubicacion"), rs.getInt("fuera_servicio") == 1));
            }
        }
        return lista;
    }

    /**
     * ¿Está dado de alta este operario? (para validar los FO cuando llegue Kafka)
     */
    public synchronized boolean existeOperario(String id) {
        try (PreparedStatement ps = con.prepareStatement("SELECT 1 FROM operario WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            error("comprobar el operario " + id, e);
            return false;
        }
    }

    /**
     * Guarda la nueva ubicación que ha enviado una WS en su AUTH.
     */
    public synchronized void actualizarUbicacion(String id, String ubicacion) {
        try (PreparedStatement ps = con.prepareStatement("UPDATE estacion SET ubicacion = ? WHERE id = ?")) {
            ps.setString(1, ubicacion);
            ps.setString(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            error("actualizar la ubicación de " + id, e);
        }
    }

    /**
     * Guarda si una estación está bloqueada por la Central (sobrevive a reinicios).
     */
    public synchronized void guardarBloqueo(String id, boolean fueraServicio) {
        try (PreparedStatement ps = con.prepareStatement("UPDATE estacion SET fuera_servicio = ? WHERE id = ?")) {
            ps.setInt(1, fueraServicio ? 1 : 0);
            ps.setString(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            error("guardar el bloqueo de " + id, e);
        }
    }

    /**
     * Un fallo de escritura no debe tumbar la Central: se registra y se sigue.
     */
    private void error(String accion, SQLException e) {
        System.err.println("[BD] Error al " + accion + ": " + e.getMessage());
    }
}
