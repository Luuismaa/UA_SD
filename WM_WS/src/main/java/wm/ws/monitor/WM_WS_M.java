package wm.ws.monitor;

import wm.common.interfaces.INetworkClient;
import wm.common.network.SocketClient;

import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Monitor de una estación de riego (WM_WS_M).
 *
 * Rol cliente (este bloque): se conecta a WM_Central, se autentica con AUTH#idWS#ubicacion,
 * envía un LATIDO cada segundo y, si pierde la conexión, reconecta y se vuelve a autenticar solo.
 * Rol servidor (siguiente fase): espera la conexión de su Engine y le comprueba la salud cada
 * segundo; desde ahí se llamará a notificarFuga() / notificarFugaResuelta().
 */
public class WM_WS_M {
    /**
     * Cada cuánto se envía un LATIDO a la Central (la Central da la WS por caída a los 3 s)
     */
    private static final int LATIDO_MS = 1000;
    /**
     * Espera entre intentos de conexión cuando la Central no está disponible
     */
    private static final int REINTENTO_MS = 3000;
    /**
     * Puerto de escucha del servidor de sockets que ofrece a WM_WS_E
     */
    private final int puertoEscucha;
    /**
     * IP de la Central a la que se conectará esta WS
     */
    private final String ipCentral;
    /**
     * Puerto de la Central a la que se conectará esta WS
     */
    private final int puertoCentral;
    /**
     * ID de la WS: Identificador único con que esa WS está dada de alta en la red (formato WS-03).
     */
    private final String idWS;
    /**
     * Ubicación de la WS (parque o jardín). Se envía a la Central en el AUTH.
     */
    private final String ubicacion;
    /**
     * Conexión actual con la Central, ya autenticada. null mientras no estemos conectados.
     * volatile: la escribe el hilo de conexión y la lee el hilo que notifique fugas.
     */
    private volatile INetworkClient central = null;
    /**
     * ¿Hay ahora mismo una fuga activa? Se recuerda para reenviar FUGA tras reconectar:
     * al autenticarse, la Central pone la WS en DISPONIBLE y no sabría que sigue con fuga.
     */
    private volatile boolean enFuga = false;
    /**
     * Planificador de tareas: ningún hilo se queda parado esperando. En vez de dormir esperando,
     * se le pide que ejecute una tarea más tarde (reintento) o cada X tiempo (latido).
     * Tiene un solo hilo, así que la conexión y el latido nunca se ejecutan a la vez.
     */
    private final ScheduledExecutorService planificador = Executors.newSingleThreadScheduledExecutor();
    /**
     * Tarea periódica del latido, para poder cancelarla cuando se pierde la conexión.
     * Solo la usa el hilo del planificador.
     */
    private ScheduledFuture<?> tareaLatido;
    /**
     * Constructor de la clase WM_WS_M que inicializa los parámetros de configuración.
     * @param puertoEscucha Puerto de escucha del servidor de sockets que ofrece a WM_WS_E
     * @param ipCentral IP de la Central a la que se conectará esta WS
     * @param puertoCentral Puerto de la Central a la que se conectará esta WS
     * @param idWS ID de la WS (formato WS-03)
     * @param ubicacion Ubicación de la WS (sin #)
     */
    public WM_WS_M(int puertoEscucha, String ipCentral, int puertoCentral, String idWS, String ubicacion) {
        this.puertoEscucha = puertoEscucha;
        this.ipCentral = ipCentral;
        this.puertoCentral = puertoCentral;
        this.idWS = idWS;
        this.ubicacion = ubicacion;
    }

    public void start(){
      log("Iniciando monitor de la estación " + idWS + " (" + ubicacion + ")");
      //Rol cliente: primer intento de conexión con la Central, ya en el hilo del planificador
      planificador.execute(this::intentarConectar);

      //Rol servidor: Esperar conexiones de WM_WS_E (siguiente fase, en el puerto puertoEscucha)
    }

    /**
     * UN intento de conectar y autenticarse en la Central.
     *  - Si sale bien, programa el latido cada segundo.
     *  - Si falla, programa el siguiente intento dentro de REINTENTO_MS y termina (no espera).
     */
    private void intentarConectar(){
      SocketClient cliente = new SocketClient();
      try {
        cliente.connect(ipCentral, puertoCentral);
        cliente.sendMessage("AUTH#" + idWS + "#" + ubicacion);
        String respuesta = cliente.receiveMessage();
        if (respuesta == null) {
          throw new IOException("la Central ha cerrado la conexión sin responder al AUTH");
        }

        String[] campos = respuesta.split("#", -1);
        if (campos[0].equals("AUTH_OK")) {
          log("Autenticada en WM_Central");
          //Primero publicamos la conexión y después miramos enFuga: si se notifica una fuga
          //justo en medio, como mucho se envía dos veces (inofensivo), pero nunca se pierde
          central = cliente;
          if (enFuga) {
            log("Fuga todavía activa: se reenvía FUGA a la Central");
            cliente.sendMessage("FUGA#" + idWS);
          }
          //A partir de ahora el planificador envía un LATIDO cada segundo
          tareaLatido = planificador.scheduleAtFixedRate(this::enviarLatido, LATIDO_MS, LATIDO_MS, TimeUnit.MILLISECONDS);
          return;

        } else if (campos[0].equals("AUTH_KO")) {
          String motivo = campos.length > 2 ? campos[2] : "DESCONOCIDO";
          if (!motivo.equals("ID_DUPLICADO")) {
            //NO_REGISTRADA o FORMATO_INVALIDO: reintentar no lo arregla
            System.err.println("[" + LocalTime.now().withNano(0) + "] [MONITOR " + idWS + "] WM_Central rechaza la estación: " + motivo + ". Terminando.");
            cliente.disconnect();
            System.exit(1);
          }
          //ID_DUPLICADO: puede ser nuestra conexión anterior, que la Central tarda hasta 3 s en soltar
          log("WM_Central indica ID_DUPLICADO. Reintentando en " + (REINTENTO_MS / 1000) + " s...");

        } else {
          log("Respuesta inesperada al AUTH: '" + respuesta + "'");
        }

      } catch (IOException e) {
        log("Imposible conectar con WM_Central (" + e.getMessage() + "). Reintentando en " + (REINTENTO_MS / 1000) + " s...");
      }
      //Si llegamos aquí, el intento ha fallado
      central = null;
      cliente.disconnect();
      programarReintento();
    }

    /**
     * Tarea periódica: envía un LATIDO. Si falla, la conexión se ha perdido: se cancela el
     * latido y se programa la reconexión.
     * Importante: no deja escapar ninguna excepción, porque si una tarea de scheduleAtFixedRate
     * lanza una excepción, el planificador deja de ejecutarla para siempre sin avisar.
     */
    private void enviarLatido(){
      INetworkClient c = central;
      if (c == null) {
        return;
      }
      try {
        c.sendMessage("LATIDO#" + idWS);
      } catch (IOException e) {
        log("Conexión con WM_Central perdida (" + e.getMessage() + "). Reintentando en " + (REINTENTO_MS / 1000) + " s...");
        tareaLatido.cancel(false);
        central = null;
        c.disconnect();
        programarReintento();
      }
    }

    /**
     * Programa un nuevo intento de conexión dentro de REINTENTO_MS (sin bloquear ningún hilo).
     */
    private void programarReintento(){
      planificador.schedule(this::intentarConectar, REINTENTO_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Avisa a la Central de que hay una fuga (el Engine responde KO o no responde).
     * Si ahora mismo no hay conexión, se enviará al reconectar (gracias a enFuga).
     */
    public void notificarFuga(){
      enFuga = true;
      log("FUGA detectada");
      enviarACentral("FUGA#" + idWS);
    }

    /**
     * Avisa a la Central de que la fuga se ha resuelto (el Engine vuelve a responder OK).
     */
    public void notificarFugaResuelta(){
      enFuga = false;
      log("Fuga resuelta");
      enviarACentral("FUGA_RESUELTA#" + idWS);
    }

    /**
     * Envía un mensaje a la Central si estamos conectados. Si falla no reintenta aquí:
     * el hilo de conexión detectará la caída con el siguiente latido y reconectará.
     */
    private void enviarACentral(String mensaje){
      INetworkClient c = central; //Copia local: otro hilo puede ponerla a null en cualquier momento
      if (c == null) {
        log("Sin conexión con WM_Central: '" + mensaje + "' no se ha podido enviar");
        return;
      }
      try {
        c.sendMessage(mensaje);
      } catch (IOException e) {
        log("No se ha podido enviar '" + mensaje + "' a WM_Central: " + e.getMessage());
      }
    }

    private void log(String texto){
      System.out.println("[" + LocalTime.now().withNano(0) + "] [MONITOR " + idWS + "] " + texto);
    }

    /**
     * Verifica los argumentos de entrada y crea el monitor. Si algo no es válido, termina.
     * @param args Argumentos recibidos por consola al iniciar la aplicación.
     */
    private static WM_WS_M crearDesdeArgs(String[] args){
      String uso = "ERR: <Puerto local para WS_E> <IP:Puerto Central> <ID de la WS (WS-03)> <Ubicación>";
      if (args.length != 4 || !args[1].contains(":")) {
        System.err.println(uso);
        System.exit(1);
      }
      try{
        int puertoEscucha = Integer.parseInt(args[0]);
        String[] ipPuerto = args[1].split(":");
        String ipCentral = ipPuerto[0];
        int puertoCentral = Integer.parseInt(ipPuerto[1]);
        String idWS = args[2];
        String ubicacion = args[3].trim();

        if (!idWS.matches("WS-\\d{2}")) {
          System.err.println("ERR: El ID de la WS debe tener el formato WS- y dos dígitos (p. ej. WS-03)");
          System.exit(1);
        }
        //El # es el separador de campos del protocolo: no puede aparecer dentro de un campo
        if (ubicacion.isEmpty() || ubicacion.contains("#")) {
          System.err.println("ERR: La ubicación no puede estar vacía ni contener '#'");
          System.exit(1);
        }
        return new WM_WS_M(puertoEscucha, ipCentral, puertoCentral, idWS, ubicacion);
      }catch(NumberFormatException e){
        System.err.println("ERR: Puerto debe ser un número. " + e.getMessage());
        System.exit(1);
        return null; //Nunca se llega aquí: System.exit termina el programa
      }
    }
    /**
     * El orden de los parámetros de entrada por línea de comandos es:
     * 1. Puerto del servidor de sockets que ofrece a WM_WS_E
     * 2. IP y Puerto de la Central en formato "IP:Puerto" (ej. "127.0.0.1:5050").
     * 3. ID de la WS: Identificador único con que esa WS está dada de alta en la red (ej. "WS-03").
     * 4. Ubicación de la WS, entre comillas si tiene espacios (ej. "Parque Central").
     *
     * @param args Argumentos recibidos por consola al iniciar la aplicación.
     */
    public static void main(String[] args){
      WM_WS_M monitor = crearDesdeArgs(args);
      monitor.start();
    }
}
