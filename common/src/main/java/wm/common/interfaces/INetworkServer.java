package wm.common.interfaces;

import java.io.IOException;

/**
 *Permite abrir un puerto de escucha, recibir conexiones de clientes y gestionar el cierre de red.
 */
public interface INetworkServer {
    /**
     * Inicia el servidor de red en el puerto especificado y queda a la escucha de peticiones entrantes de clientes.
     * Este método BLOQUEA: no vuelve hasta que se llama a stopServer().
     * @param port El puerto en el que el servidor escuchará las conexiones entrantes.
     * @throws IOException si no se puede abrir el puerto (p. ej. ya está en uso)
     */
    void startServer(int port) throws IOException;
    /**
     * Detiene el servidor de red, cerrando todas las conexiones activas y liberando los recursos asociados al servidor.
     */
    void stopServer();
}
