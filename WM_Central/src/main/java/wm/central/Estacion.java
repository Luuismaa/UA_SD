package wm.central;

import wm.common.protocol.EstadoWS;

/**
 * Ficha de una estación de riego tal y como la conoce la Central.
 *
 * Solo RegistroEstaciones modifica estos datos (por eso los setters no son públicos):
 * así todos los cambios de estado pasan por un único sitio sincronizado.
 * Los campos que cambian son volatile para que cualquier hilo que los lea (p. ej. el panel)
 * vea siempre el último valor escrito por otro hilo.
 */
public class Estacion {

    private final String id;
    private volatile String ubicacion;

    //Hasta que su Monitor no se autentica, la Central no sabe el estado real (punto 1 de la mecánica)
    private volatile EstadoWS estado = EstadoWS.DESCONECTADA;

    //¿Hay ahora mismo un Monitor conectado y autenticado con este ID?
    private volatile boolean conectada = false;

    //¿La ha bloqueado la Central? Se guarda en la BD y sobrevive a reinicios. Mientras esté
    //bloqueada, al conectarse o al resolver una fuga pasa a FUERA_SERVICIO en vez de DISPONIBLE
    private volatile boolean bloqueada;

    public Estacion(String id, String ubicacion, boolean bloqueada) {
        this.id = id;
        this.ubicacion = ubicacion;
        this.bloqueada = bloqueada;
    }

    public String getId() {
        return id;
    }

    public String getUbicacion() {
        return ubicacion;
    }

    public EstadoWS getEstado() {
        return estado;
    }

    public boolean isConectada() {
        return conectada;
    }

    public boolean isBloqueada() {
        return bloqueada;
    }

    //Setters de paquete: solo los usa RegistroEstaciones

    void setUbicacion(String ubicacion) {
        this.ubicacion = ubicacion;
    }

    void setEstado(EstadoWS estado) {
        this.estado = estado;
    }

    void setConectada(boolean conectada) {
        this.conectada = conectada;
    }

    void setBloqueada(boolean bloqueada) {
        this.bloqueada = bloqueada;
    }

    @Override
    public String toString() {
        return id + " (" + ubicacion + ") " + estado + (bloqueada ? " [bloqueada]" : "");
    }
}
