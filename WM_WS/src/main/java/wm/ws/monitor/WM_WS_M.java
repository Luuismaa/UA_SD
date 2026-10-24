package wm.ws.monitor;

import wm.common.interfaces.INetworkClient;
import wm.common.network.SocketClient;

public class WM_WS_M {
    /**
     * Puerto de escucha del servidor de sockets que ofrece a WM_WS_E
     */
    private static int puertoEscucha;
    /**
     * IP de la Central a la que se conectará esta WS
     */
    private static String ipCentral;
    /**
     * Puerto de la Central a la que se conectará esta WS
     */
    private static int puertoCentral;
    /**
     * ID de la WS: Identificador único con que esa WS está dada de alta en la red. (ej. "M1").
     */
    private static String idWS;
    /**
     * Cliente de red que se utilizará para conectarse a la Central y enviar/recibir mensajes.
     */
    private INetworkClient networkClient;
    /**
     * Constructor de la clase WM_WS_M que inicializa los parámetros de configuración y el cliente de red.
     * @param puertoEscucha Puerto de escucha del servidor de sockets que ofrece a WM_WS_E
     * @param ipCentral IP de la Central a la que se conectará esta WS
     * @param puertoCentral Puerto de la Central a la que se conectará esta WS
     * @param idWS ID de la WS: Identificador único con que esa WS está dada de alta en la red. (ej. "M1").
     */
    public WM_WS_M(int puertoEscucha, String ipCentral, int puertoCentral, String idWS) {
        this.puertoEscucha = puertoEscucha;
        this.ipCentral = ipCentral;
        this.puertoCentral = puertoCentral;
        this.idWS = idWS;
        
        // Instanciamos el cliente que ya programamos
        this.networkClient = new SocketClient();
    }

    public void start(){
      System.out.println("Iniciando monitor de la estación: " + idWS);
      //Rol cliente: Conectarse a la Central y registrarse
      boolean connect = connectToCentral();
      if(!connect){
        System.err.println("No se pudo conectar a la Central. Terminando ejecución.");
        System.exit(1);
      } else {
        System.out.println("Conexión a la Central establecida correctamente.");
      }
      //Rol servidor: Esperar conexiones de WM_WS_E
    }

    private boolean connectToCentral(){
      networkClient.connect(ipCentral, puertoCentral);
      String mensajeRegistro = "REGISTRO#" + idWS;
      System.out.println("Enviando mensaje de registro a la Central: " + mensajeRegistro);
      networkClient.sendMessage(mensajeRegistro);
      //Esperamos la respuesta de la Central
      String respuesta = networkClient.receiveMessage();
      if(respuesta.equals("OK")) {
        System.out.println("Registro exitoso en la Central.");
        return true;
      } else {
        System.err.println("Error al registrar en la Central: " + respuesta);
        return false;
      }
    }
    /**
     * Verifica que los argumentos de entrada sean correctos y que cumplan con el formato esperado.
     * @param args Argumentos recibidos por consola al iniciar la aplicación.
     */
    private static void checkArgs(String[] args){
      if(args.length != 3 || !args[1].contains(":")) {
				System.err.println("ERR: <Puerto local para WS_E> <IP:Puerto Central> <ID de la WS>");
				System.exit (1);
      }
      try{
        puertoEscucha = Integer.parseInt(args[0]);
        String[] ipPuerto = args[1].split(":");
        ipCentral = ipPuerto[0];
        puertoCentral = Integer.parseInt(ipPuerto[1]);
        idWS = args[2];
      }catch(NumberFormatException e){
        System.err.println("ERR: Puerto debe ser un número. " + e.getMessage());
        System.exit(1);
      }
      
    }
    /**
     * El orden de los parámetros de entrada por línea de comandos es:
     * 1. Puerto del servidor de sockets que ofrece a WM_WS_E
     * 2. IP y Puerto de la Central en formato "IP:Puerto" (ej. "127.0.0.1:8080").
     * 3. ID de la WS: Identificador único con que esa WS está dada de alta en la red. (ej. "M1").
     * 
     * @param args Argumentos recibidos por consola al iniciar la aplicación.
     */
    public static void main(String[] args){
      checkArgs(args);
      WM_WS_M monitor = new WM_WS_M(puertoEscucha, ipCentral, puertoCentral, idWS);
      monitor.start();
    }
}
