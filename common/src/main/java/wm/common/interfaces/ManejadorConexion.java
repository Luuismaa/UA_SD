package wm.common.interfaces;

import java.net.Socket;

/**
 * Qué hacer con cada conexión que acepta un SocketServer.
 * El servidor solo sabe aceptar conexiones; quien lo crea le pasa un ManejadorConexion
 * con la lógica concreta (la Central atiende Monitors, el Monitor atiende a su Engine).
 */
public interface ManejadorConexion {
    /**
     * Atiende una conexión de principio a fin. Se ejecuta en un hilo propio por cada cliente,
     * así que puede bloquearse leyendo sin afectar a los demás.
     * Cuando este método termina (normalmente o con excepción), el servidor cierra el socket.
     * @param socket conexión recién aceptada con el cliente
     */
    void atender(Socket socket);
}
