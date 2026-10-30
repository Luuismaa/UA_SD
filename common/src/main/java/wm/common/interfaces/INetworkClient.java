package wm.common.interfaces;
/**
 * Esta interfaz define los métodos que debe implementar un cliente de red para conectarse a un servidor, enviar y recibir mensajes, y desconectarse.
 * 
 */
public interface INetworkClient{
    /**
     * Establece una conexión con el servidor especificado por el host y el puerto.
     * @param ip Dirección IP del servidor al que se desea conectar.
     * @param port Puerto de escucha del servidor
     */
    void connect(String ip, int port);
    /**
     * Envía un mensaje al servidor al que está conectado el cliente.
     * @param message El mensaje que se desea enviar al servidor. (PING, KO)
     */
    void sendMessage(String message);
    /**
     * Espera hasta recibir un mensaje del servidor
     * @return El mensaje recibido del servidor.
     */
    String receiveMessage();
    /**
     * Cierra la conexión con el servidor y libera los recursos asociados al cliente de red.
     */
    void disconnect();
}