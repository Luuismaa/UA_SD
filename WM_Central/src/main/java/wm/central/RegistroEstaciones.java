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
 * Varios hilos lo usan a la vez (un ManejadorMonitor por cada Monitor conectado y, más
 * adelante, el consumidor de Kafka y el menú). Por eso:
 *  - El mapa es un ConcurrentHashMap: se puede recorrer mientras otros hilos lo modifican.
 *  - Los métodos que cambian estados son synchronized: cada uno comprueba y modifica sin
 *    que otro hilo se cuele en medio.
 *
 * al panel y publicar ESTADO_WS por Kafka.
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

    private KafkaCentralManager kafkaManager;

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
     */
    public synchronized void marcarFuga(String id) {
        Estacion est = estaciones.get(id);
        if (est == null || !est.isConectada()) {
            return;
        }
        if(kafkaManager != null){
            String idFO = kafkaManager.getOperarioRegando(id);
            if(idFO != null){
                kafkaManager.publicarEvento(Topics.FUGA_RIEGO, idFO, "¡ALERTA FUGA en estación " + id + "!");
            }
            //Protocolo: PARAR#<idWS>#FUGA
            kafkaManager.publicarEvento(Topics.ORDENES_RIEGO, id, "PARAR#" + id + "#FUGA");
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
        if(kafkaManager != null){
            //Protocolo: BLOQUEAR#<idWS>
            kafkaManager.publicarEvento(Topics.ORDENES_RIEGO, id, "BLOQUEAR#" + id);
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
        if(kafkaManager != null){
            kafkaManager.publicarEvento(Topics.ORDENES_RIEGO, id, "ACTIVAR#" + id);
        }
        if (est.getEstado() == EstadoWS.FUERA_SERVICIO) {
            cambiarEstado(est, EstadoWS.DISPONIBLE);
        } else {
            log(id, "activada");
        }
    }

    /**
     * Estado de una estación conectada y sin fuga: DISPONIBLE, o FUERA_SERVICIO si está bloqueada.
     */
    private EstadoWS estadoOperativo(Estacion est) {
        return est.isBloqueada() ? EstadoWS.FUERA_SERVICIO : EstadoWS.DISPONIBLE;
    }

    /**
     * La conexión con el Monitor se ha perdido (EOT, caída o sin latidos).
     */
    public synchronized void desconectar(String id) {
        Estacion est = estaciones.get(id);
        if (est == null) {
            return;
        }
        //Si estaba regando avisamos al operario de la desconexion
        if(est.getEstado() == EstadoWS.REGANDO && kafkaManager != null){
            String idFO = kafkaManager.getOperarioRegando(id);
            if(idFO !=null){
                kafkaManager.publicarEvento(Topics.RESUMEN_RIEGO, idFO, "Resumen: Estación " + id + 
                    " | Motivo: CENTRAL | Riego abortado por desconexión de red."
                );
                kafkaManager.liberarOperario(id);
            }
        }
        est.setConectada(false);
        cambiarEstado(est, EstadoWS.DESCONECTADA);
    }
    /**
     * Todas las estaciones (para el panel). Es una vista del mapa: refleja los cambios
     * y se puede recorrer aunque otros hilos estén modificando estados.
     */
    public Collection<Estacion> getEstaciones() {
        return estaciones.values();
    }

    public void cambiarEstadoPublico(Estacion est, EstadoWS nuevo){
        cambiarEstado(est, nuevo);
    }

    /**
     * Único punto donde cambia el estado de una estación.
     */
    private void cambiarEstado(Estacion est, EstadoWS nuevo) {
        EstadoWS anterior = est.getEstado();
        if (anterior == nuevo) {
            return;
        }
        est.setEstado(nuevo);
        log(est.getId(), anterior + " -> " + nuevo);
        if(kafkaManager != null){
            String mensaje = String.format("ESTADO_WS#%s#%s#%s", est.getId(), est.getUbicacion(), nuevo.name());
            kafkaManager.publicarEvento(Topics.ESTADO_GLOBAL_WS, est.getId(), mensaje);
        }
    }

    private void log(String id, String texto) {
        System.out.println("[" + LocalTime.now().withNano(0) + "] [" + id + "] " + texto);
    }
    public void setKafkaManager(KafkaCentralManager kafkaManager){
        this.kafkaManager = kafkaManager;
    }
}
