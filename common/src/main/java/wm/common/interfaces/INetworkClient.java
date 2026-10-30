package wm.common.interfaces;

import java.io.IOException;

/**
 * Esta interfaz define los métodos que debe implementar un cliente de red para conectarse a un servidor, enviar y recibir mensajes, y desconectarse.
 * Los métodos lanzan IOException en vez de tragarse los errores: quien los usa necesita
 * enterarse de que la conexión ha fallado para reintentar o avisar al usuario.
 */
public interface INetworkClient{
    /**
     * Establece una conexión con el servidor y hace el saludo inicial (ENQ -> ACK).
     * @param ip Dirección IP del servidor al que se desea conectar.
     * @param port Puerto de escucha del servidor
     * @throws IOException si no se puede conectar o el servidor no responde ACK al ENQ
     */
    void connect(String ip, int port) throws IOException;
    /**
     * Envía un mensaje al servidor y espera su ACK (reenvía si recibe NACK).
     * @param message El mensaje que se desea enviar al servidor (CODIGO#campo1#...).
     * @throws IOException si la conexión falla
     */
    void sendMessage(String message) throws IOException;
    /**
     * Espera hasta recibir un mensaje del servidor
     * @return El mensaje recibido del servidor, o null si el servidor se ha despedido con EOT.
     * @throws IOException si la conexión falla
     */
    String receiveMessage() throws IOException;
    /**
     * Se despide del servidor (EOT) y cierra la conexión. Nunca lanza excepción.
     */
    void disconnect();
}
