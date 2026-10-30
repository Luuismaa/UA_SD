package wm.common.interfaces;
/**
 *Permite abrir un puerto de escucha, recibir conexiones de clientes y gestionar el cierre de red.
 */
public interface INetworkServer {
    /**
     * Inicia el servidor de red en el puerto especificado y queda a la escucha de peticiones entrantes de clientes.
     * @param port El puerto en el que el servidor escuchará las conexiones entrantes.
     */
    void startServer(int port);
    /**
     * Detiene el servidor de red, cerrando todas las conexiones activas y liberando los recursos asociados al servidor.
     */
    void stopServer();
}
