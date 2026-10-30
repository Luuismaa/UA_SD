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

    public Estacion(String id, String ubicacion) {
        this.id = id;
        this.ubicacion = ubicacion;
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

    @Override
    public String toString() {
        return id + " (" + ubicacion + ") " + estado;
    }
}
