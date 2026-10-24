package wm.common.network;
import wm.common.interfaces.INetworkClient;
import wm.common.protocol.SocketProtocol;
import java.net.*;
import java.io.*;
import java.util.Arrays;
/**
 * Clase que implementa la interfaz INetworkClient para establecer una conexión de red mediante sockets.
 * Esta clase permite conectarse a un servidor, enviar y recibir mensajes, y desconectarse de manera sencilla.
 * SocketClient
 */
public class SocketClient implements INetworkClient{
    /**
     * Socket utilizado para la comunicación con el servidor.
     */
    private Socket socket;
    /**
     * Flujo de salida para enviar datos al servidor.
     */
    private DataOutputStream out;
    /**
     * Flujo de entrada para recibir datos del servidor.
     */
    private DataInputStream in;
    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkClient#connect()
     */
    @Override 
    public void connect(String ip, int port){
        try {
            this.socket = new Socket(ip, port);
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
            System.out.println("Conectado al servidor en " + ip + ":" + port);
        } catch (IOException e) {
            System.err.println("Error al conectar con el servidor: " + e.getMessage());
        }
    }
    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkClient#disconnect()
     */
    @Override
    public void disconnect(){
        try {
            if (this.in != null) {
                this.in.close();
            }
            if (this.out != null) {
                this.out.close();
            }
            if (this.socket != null) {
                this.socket.close();
            }
            System.out.println("Desconectado del servidor.");
        } catch (IOException e) {
            System.err.println("Error al desconectar del servidor: " + e.getMessage());
        }
    }
    /**    (non-Javadoc)
     * 
     * @see wm.common.interfaces.INetworkClient#receiveMessage()
     */
    @Override
    public String receiveMessage(){
        if(in != null){
            byte [] buffer = new byte[4096]; // Tamaño del buffer para leer datos
            int indice = 0;
            try{
                //Leemos el primer byte
                int b = in.read();
                if(b == -1){
                    return null; // Fin de flujo
                }
                if (b != SocketProtocol.STX) {
                    System.err.println("[SOCKET_CLIENT] Error: La respuesta no empieza por STX");
                    return null;
                }
                //Guardamos el STX
                buffer[indice++] = (byte) b;
                //Leemos DATA hasta encontrar ETX
                while ((b = in.read()) != -1) {
                    if(indice >= buffer.length){
                        System.err.println("[SOCKET_CLIENT] Error: Buffer overflow, mensaje demasiado largo");
                        return null;
                    }
                    buffer[indice++] = (byte) b;
                    if (b == SocketProtocol.ETX) {
                        //El siguiente byte es el LRC, lo leemos y lo guardamos
                        buffer[indice++] = (byte) in.read();
                        break; // Encontramos ETX, salimos del bucle
                    }
                }
                //Recortamos el buffer para quedarnos solo con los bytes útiles
                byte[] frame = Arrays.copyOfRange(buffer, 0, indice);
                //Desempaquetamos
                return SocketProtocol.parseFrame(frame);
            }catch(IOException e){
                System.err.println("Error al recibir mensaje: " + e.getMessage());
                return null;
            }catch(IllegalArgumentException e){
                System.err.println("Error al parsear frame: " + e.getMessage());
                return null;
            }
        }
        return null;
    }
    /**    (non-Javadoc)
     * @see wm.common.interfaces.INetworkClient#sendMessage()
     */
    @Override
    public void sendMessage (String p_Datos){
        if(out!=null){
            try{
                byte[] mensaje = SocketProtocol.buildFrame(p_Datos);
                out.write(mensaje);
                out.flush();
            }catch(IOException e){
                System.err.println("Error al enviar mensaje: " + e.getMessage());
            }
        }
    }
}
