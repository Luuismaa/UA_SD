package wm.central;

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
 * TODO cambio de estado pasa por cambiarEstado(): es el único punto donde habrá que avisar
 * al panel y publicar ESTADO_WS por Kafka.
 */
public class RegistroEstaciones {

    /**
     * Resultado de una autenticación. El nombre de cada valor es el motivo que viaja en
     * AUTH_KO (p. ej. AUTH_KO#WS-07#NO_REGISTRADA).
     */
    public enum ResultadoAuth { OK, NO_REGISTRADA, ID_DUPLICADO }

    private final Map<String, Estacion> estaciones = new ConcurrentHashMap<>();

    /**
     * Da de alta una estación conocida (al arrancar, desde la BD). Empieza DESCONECTADA.
     */
    public void registrar(String id, String ubicacion) {
        estaciones.put(id, new Estacion(id, ubicacion));
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
        //La WS informa de su ubicación al registrarse: nos quedamos con la que envía
        if (!est.getUbicacion().equals(ubicacion)) {
            log(id, "ubicación actualizada: '" + est.getUbicacion() + "' -> '" + ubicacion + "'");
            est.setUbicacion(ubicacion);
        }
        est.setConectada(true);
        cambiarEstado(est, EstadoWS.DISPONIBLE);
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
        //TODO Kafka: si estaba REGANDO -> publicar PARAR#id#FUGA y avisar al FO por fugaRiego
        cambiarEstado(est, EstadoWS.FUGA);
    }

    /**
     * El Monitor avisa de que la fuga se ha resuelto: la WS vuelve a estar disponible.
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
        cambiarEstado(est, EstadoWS.DISPONIBLE);
    }

    /**
     * La conexión con el Monitor se ha perdido (EOT, caída o sin latidos).
     */
    public synchronized void desconectar(String id) {
        Estacion est = estaciones.get(id);
        if (est == null) {
            return;
        }
        //TODO Kafka: si estaba REGANDO -> cerrar el riego y enviar RESUMEN al FO con motivo CENTRAL
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
        //TODO Kafka: publicar ESTADO_WS#id#ubicacion#estado en estadoGlobalWS
    }

    private void log(String id, String texto) {
        System.out.println("[" + LocalTime.now().withNano(0) + "] [" + id + "] " + texto);
    }
}
