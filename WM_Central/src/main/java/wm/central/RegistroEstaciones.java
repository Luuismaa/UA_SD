package wm.central;

import wm.common.kafka.Topics;
import wm.common.protocol.EstadoWS;

import java.time.LocalTime;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Estado compartido de todas las estaciones que conoce la Central.
 *
 * Varios hilos lo usan a la vez (un ManejadorMonitor por cada Monitor conectado, el consumidor
 * de Kafka y la interfaz). Por eso:
 *  - El mapa es un ConcurrentHashMap: se puede recorrer mientras otros hilos lo modifican.
 *  - Los métodos que cambian estados son synchronized: cada uno comprueba y modifica sin
 *    que otro hilo se cuele en medio.
 *
 * Todo cambio de estado pasa por cambiarEstado(): es el único punto donde se publica
 * ESTADO_WS por Kafka.
 */
public class RegistroEstaciones {

    /**
     * Resultado de una autenticación. El nombre de cada valor es el motivo que viaja en
     * AUTH_KO (p. ej. AUTH_KO#WS-07#NO_REGISTRADA).
     */
    public enum ResultadoAuth { OK, NO_REGISTRADA, ID_DUPLICADO }

    private final Map<String, Estacion> estaciones = new ConcurrentHashMap<>();

    //Donde se guardan los cambios que deben sobrevivir a un reinicio (ubicación y bloqueo)
    private final BaseDatos bd;

    //null mientras Kafka no esté disponible: la Central sigue funcionando por sockets
    private volatile KafkaCentralManager kafkaManager;

    public RegistroEstaciones(BaseDatos bd) {
        this.bd = bd;
    }

    /**
     * Da de alta una estación conocida (al arrancar, desde la BD). Empieza DESCONECTADA.
     */
    public void registrar(String id, String ubicacion, boolean bloqueada) {
        estaciones.put(id, new Estacion(id, ubicacion, bloqueada));
    }

    /**
     * La estación con ese id, o null si no está registrada.
     */
    public Estacion getEstacion(String id) {
        return estaciones.get(id);
    }

    /**
     * Intenta autenticar el Monitor de una estación (mensaje AUTH).
     * Solo se aceptan IDs registrados y que no tengan ya un Monitor conectado.
     */
    public synchronized ResultadoAuth autenticar(String id, String ubicacion) {
        Estacion est = estaciones.get(id);
        if (est == null) {
            return ResultadoAuth.NO_REGISTRADA;
        }
        if (est.isConectada()) {
            return ResultadoAuth.ID_DUPLICADO;
        }
        //La WS informa de su ubicación al registrarse: nos quedamos con la que envía (y en la BD)
        if (!est.getUbicacion().equals(ubicacion)) {
            log(id, "ubicación actualizada: '" + est.getUbicacion() + "' -> '" + ubicacion + "'");
            est.setUbicacion(ubicacion);
            bd.actualizarUbicacion(id, ubicacion);
        }
        est.setConectada(true);
        cambiarEstado(est, estadoOperativo(est));
        return ResultadoAuth.OK;
    }

    /**
     * El Monitor avisa de una fuga (su Engine responde KO o no responde).
     * Si estaba regando: PARAR#id#FUGA a la WS y aviso FUGA al operario (lo hace el gestor Kafka).
     */
    public synchronized void marcarFuga(String id) {
        Estacion est = estaciones.get(id);
        if (est == null || !est.isConectada()) {
            return;
        }
        KafkaCentralManager k = kafkaManager;
        if (k != null) {
            k.alDetectarFuga(id);
        }
        cambiarEstado(est, EstadoWS.FUGA);
    }

    /**
     * El Monitor avisa de que la fuga se ha resuelto: la WS vuelve a estar disponible
     * (o FUERA_SERVICIO si la Central la tiene bloqueada).
     */
    public synchronized void resolverFuga(String id) {
        Estacion est = estaciones.get(id);
        if (est == null || !est.isConectada()) {
            return;
        }
        if (est.getEstado() != EstadoWS.FUGA) {
            log(id, "FUGA_RESUELTA ignorado: la estación no estaba en FUGA (" + est.getEstado() + ")");
            return;
        }
        cambiarEstado(est, estadoOperativo(est));
    }

    /**
     * La Central bloquea la estación: queda FUERA_SERVICIO (también tras reiniciar la Central).
     * Si estaba regando, el Engine para y publica FIN con motivo BLOQUEO.
     * Si está desconectada, se guarda el bloqueo y se aplicará cuando se conecte.
     * Si tiene una fuga, sigue en FUGA y pasará a FUERA_SERVICIO al resolverse.
     */
    public synchronized void bloquear(String id) {
        Estacion est = estaciones.get(id);
        if (est == null) {
            return;
        }
        est.setBloqueada(true);
        bd.guardarBloqueo(id, true);
        KafkaCentralManager k = kafkaManager;
        if (k != null) {
            //Protocolo: BLOQUEAR#<idWS>
            k.publicarEvento(Topics.ORDENES_RIEGO, id, "BLOQUEAR#" + id);
        }
        if (est.isConectada() && est.getEstado() != EstadoWS.FUGA) {
            cambiarEstado(est, EstadoWS.FUERA_SERVICIO);
        } else {
            log(id, "bloqueada (se aplicará al conectarse o al resolver la fuga)");
        }
    }

    /**
     * La Central reactiva una estación bloqueada.
     */
    public synchronized void activar(String id) {
        Estacion est = estaciones.get(id);
        if (est == null) {
            return;
        }
        est.setBloqueada(false);
        bd.guardarBloqueo(id, false);
        KafkaCentralManager k = kafkaManager;
        if (k != null) {
            //Protocolo: ACTIVAR#<idWS>
            k.publicarEvento(Topics.ORDENES_RIEGO, id, "ACTIVAR#" + id);
        }
        if (est.getEstado() == EstadoWS.FUERA_SERVICIO) {
            cambiarEstado(est, EstadoWS.DISPONIBLE);
        } else {
            log(id, "activada");
        }
    }

    /**
     * La conexión con el Monitor se ha perdido (EOT, caída o sin latidos).
     * Si estaba regando, la Central cierra el riego con los últimos datos y envía RESUMEN
     * con motivo CENTRAL (flujo 5 del protocolo).
     */
    public synchronized void desconectar(String id) {
        Estacion est = estaciones.get(id);
        if (est == null) {
            return;
        }
        est.setConectada(false);
        KafkaCentralManager k = kafkaManager;
        if (k != null) {
            k.alDesconectarEstacion(id);
        }
        cambiarEstado(est, EstadoWS.DESCONECTADA);
    }

    /**
     * La WS ha confirmado con INICIO que ha abierto la electroválvula: pasa a REGANDO.
     * @return false si la estación ya no está disponible (p. ej. le ha entrado una fuga)
     */
    public synchronized boolean iniciarRiego(String id, String operario) {
        Estacion est = estaciones.get(id);
        if (est == null || !est.isConectada() || est.getEstado() != EstadoWS.DISPONIBLE) {
            return false;
        }
        est.setOperario(operario);
        est.setCaudal(0.0);
        est.setVolumen(0.0);
        cambiarEstado(est, EstadoWS.REGANDO);
        return true;
    }

    /**
     * Telemetría de un riego en curso (para el panel).
     */
    public void actualizarTelemetria(String id, double caudal, double volumen) {
        Estacion est = estaciones.get(id);
        if (est != null) {
            est.setCaudal(caudal);
            est.setVolumen(volumen);
        }
    }

    /**
     * El riego ha terminado: se borra el caudal y el operario, y si seguía REGANDO vuelve a
     * su estado operativo. Si entretanto pasó a FUGA, FUERA_SERVICIO o DESCONECTADA, se respeta.
     */
    public synchronized void finalizarRiego(String id) {
        Estacion est = estaciones.get(id);
        if (est == null) {
            return;
        }
        est.setCaudal(0.0);
        est.setOperario(null);
        if (est.getEstado() == EstadoWS.REGANDO) {
            cambiarEstado(est, est.isConectada() ? estadoOperativo(est) : EstadoWS.DESCONECTADA);
        }
    }

    /**
     * Todas las estaciones (para el panel). Es una vista del mapa: refleja los cambios
     * y se puede recorrer aunque otros hilos estén modificando estados.
     */
    public Collection<Estacion> getEstaciones() {
        return estaciones.values();
    }

    public void setKafkaManager(KafkaCentralManager kafkaManager){
        this.kafkaManager = kafkaManager;
    }

    /**
     * Estado de una estación conectada y sin fuga: DISPONIBLE, o FUERA_SERVICIO si está bloqueada.
     */
    private EstadoWS estadoOperativo(Estacion est) {
        return est.isBloqueada() ? EstadoWS.FUERA_SERVICIO : EstadoWS.DISPONIBLE;
    }

    /**
     * Único punto donde cambia el estado de una estación (y donde se publica ESTADO_WS).
     */
    private void cambiarEstado(Estacion est, EstadoWS nuevo) {
        EstadoWS anterior = est.getEstado();
        if (anterior == nuevo) {
            return;
        }
        est.setEstado(nuevo);
        log(est.getId(), anterior + " -> " + nuevo);
        KafkaCentralManager k = kafkaManager;
        if (k != null) {
            k.publicarEstado(est);
        }
    }

    private void log(String id, String texto) {
        System.out.println("[" + LocalTime.now().withNano(0) + "] [" + id + "] " + texto);
    }
}
