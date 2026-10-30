package wm.common.network;

import wm.common.interfaces.INetworkServer;
import wm.common.interfaces.ManejadorConexion;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Servidor de sockets concurrente y genérico: acepta conexiones y atiende cada una en un hilo
 * propio. Qué se hace con cada conexión lo decide el ManejadorConexion que recibe.
 *
 * Lo usan la Central (para los Monitors) y el Monitor (para su Engine).
 */
public class SocketServer implements INetworkServer {

    //Lógica que se ejecuta para cada cliente
    private final ManejadorConexion manejador;

    //volatile: stopServer() lo cambia desde otro hilo y el bucle de accept debe verlo al momento
    private volatile boolean activo = false;

    private ServerSocket serverSocket;

    //Conexiones abiertas, para poder cerrarlas todas en stopServer().
    //Set concurrente porque los hilos de cada cliente añaden y quitan a la vez
    private final Set<Socket> conexiones = ConcurrentHashMap.newKeySet();

    public SocketServer(ManejadorConexion manejador) {
        this.manejador = manejador;
    }

    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkServer#startServer(int)
     */
    @Override
    public void startServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        activo = true;
        System.out.println("[SERVIDOR] Escuchando en el puerto " + port);

        while (activo) {
            Socket socket;
            try {
                //Bloquea hasta que se conecta un cliente
                socket = serverSocket.accept();
            } catch (IOException e) {
                //Si nos han parado, el close() de stopServer() desbloquea el accept con una excepción
                if (!activo) {
                    break;
                }
                //Fallo puntual al aceptar a un cliente: lo registramos y seguimos atendiendo a los demás
                System.err.println("[SERVIDOR] Error al aceptar conexión: " + e.getMessage());
                continue;
            }

            //Un hilo por cliente: el bucle vuelve enseguida al accept para el siguiente
            Thread hilo = new Thread(() -> atender(socket), "cliente-" + socket.getRemoteSocketAddress());
            hilo.start();
        }
        System.out.println("[SERVIDOR] Detenido");
    }

    /**
     * Ejecuta el manejador para un cliente y garantiza que el socket se cierra al terminar,
     * pase lo que pase dentro del manejador.
     */
    private void atender(Socket socket) {
        conexiones.add(socket);
        try {
            manejador.atender(socket);
        } catch (RuntimeException e) {
            //Un error inesperado en un cliente no debe tumbar el servidor ni a los demás clientes
            System.err.println("[SERVIDOR] Error inesperado atendiendo a " + socket.getRemoteSocketAddress() + ": " + e);
        } finally {
            conexiones.remove(socket);
            try {
                socket.close();
            } catch (IOException e) {
                //Ya estaba cerrado
            }
        }
    }

    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkServer#stopServer()
     */
    @Override
    public void stopServer() {
        activo = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            //Ya estaba cerrado
        }
        //Cerrar cada socket desbloquea la lectura de su hilo, que termina por su cuenta
        for (Socket s : conexiones) {
            try {
                s.close();
            } catch (IOException e) {
                //Ya estaba cerrado
            }
        }
    }
}
