package wm.common.protocol;

/**
 * Estados posibles de una estación de riego (valores cerrados del protocolo, mensaje ESTADO_WS).
 * Solo la Central decide el estado de una WS; el resto de módulos lo reciben.
 *
 * Para mensajes: EstadoWS.REGANDO.name() -> "REGANDO"  y  EstadoWS.valueOf("REGANDO") -> REGANDO
 * (valueOf lanza IllegalArgumentException si el texto no es un estado válido).
 */
public enum EstadoWS {
    //Funciona bien y espera una petición de riego (VERDE)
    DISPONIBLE,
    //Regando en este momento (VERDE parpadeante, con caudal, volumen e id del operario)
    REGANDO,
    //Conectada pero con fuga o avería: no puede regar (ROJO)
    FUGA,
    //Bloqueada por orden de la Central (NARANJA, "Fuera de Servicio")
    FUERA_SERVICIO,
    //La Central no recibe latidos de su Monitor (GRIS)
    DESCONECTADA
}
