package wm.common.network;
import wm.common.interfaces.INetworkClient;
import wm.common.protocol.SocketProtocol;
import java.net.*;
import java.io.*;
/**
 * Clase que implementa la interfaz INetworkClient para establecer una conexión de red mediante sockets.
 * Esta clase permite conectarse a un servidor, enviar y recibir mensajes, y desconectarse de manera sencilla.
 * El trabajo de empaquetar, validar y confirmar tramas lo hace ConexionesTramas: aquí solo
 * se abre el socket, se hace el saludo y se delega.
 */
public class SocketClient implements INetworkClient{
    /**
     * Conexión por tramas con el servidor (null mientras no estemos conectados).
     */
    private ConexionesTramas conexion;
    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkClient#connect()
     */
    @Override
    public void connect(String ip, int port) throws IOException {
        conexion = new ConexionesTramas(new Socket(ip, port));

        //Saludo: ENQ -> el servidor debe responder ACK
        conexion.enviarByte(SocketProtocol.ENQ);
        if (conexion.leerByte() != SocketProtocol.ACK) {
            conexion.cerrar();
            conexion = null;
            throw new IOException("El servidor no ha respondido ACK al ENQ");
        }
        System.out.println("Conectado al servidor en " + ip + ":" + port);
    }
    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkClient#disconnect()
     */
    @Override
    public void disconnect(){
        if (conexion == null) {
            return;
        }
        try {
            //Despedida ordenada: el servidor sabrá que no es una caída
            conexion.enviarByte(SocketProtocol.EOT);
        } catch (IOException e) {
            //La conexión ya estaba rota: no hay a quién despedirse
        }
        conexion.cerrar();
        conexion = null;
        System.out.println("Desconectado del servidor.");
    }
    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkClient#receiveMessage()
     */
    @Override
    public String receiveMessage() throws IOException {
        comprobarConectado();
        return conexion.recibirMensaje();
    }
    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkClient#sendMessage()
     */
    @Override
    public void sendMessage(String p_Datos) throws IOException {
        comprobarConectado();
        conexion.enviarMensaje(p_Datos);
    }
    /**
     * Evita un NullPointerException si alguien envía o recibe sin haber llamado a connect().
     */
    private void comprobarConectado() throws IOException {
        if (conexion == null) {
            throw new IOException("No hay conexión con el servidor");
        }
    }
}
