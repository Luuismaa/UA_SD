package wm.ws.engine;
import wm.common.interfaces.INetworkClient;
import wm.common.network.SocketClient;

import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * Clase principal del Engine de la WS (WM_WS_E).
 * Se encarga de iniciar el monitor y de conectarse a Kafka.
 * Engine
 */
public class Engine {
    /**
     * Puerto de Kafka al que se conectará esta WS
     */
    private final String puertoKafka;
    /**
     * IP del monitor al que se conectará esta WS
     */
    private final String ipMonitor;
    /**
     * Puerto del monitor al que se conectará esta WS
     */
    private final int puertoMonitor;
    /**
     * Indica si la WS está averiada o no.
     */
    private volatile boolean averiado = false;
    /**
     * Conexión actual con el monitor, ya autenticada. null mientras no estemos conectados.
     */
    private INetworkClient clienteMonitor;
    /**
     * Programador de tareas para reintentos de conexión al monitor
     */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    /**
     * Indica si la WS está conectada al monitor o no.
     * Se usa para que negarse a regar si no hay conexión con el monitor.
     */
    private volatile boolean monitorConectado = false;
    /**
     * Constructor de la clase Engine que inicializa los parámetros de configuración.
     * @param puertoKafka Puerto de Kafka al que se conectará esta WS
     * @param ipMonitor IP del monitor al que se conectará esta WS
     * @param puertoMonitor Puerto del monitor al que se conectará esta WS
     */
    public Engine(String puertoKafka, String ipMonitor, int puertoMonitor){
        this.puertoKafka = puertoKafka;
        this.ipMonitor = ipMonitor;
        this.puertoMonitor = puertoMonitor;
    }
    /**
     * Inicia el Engine de la WS. Se conecta al monitor y a Kafka.
     * 
     */
    public void start(){
        log("Iniciando Engine...");
        log("Kafka configurado en: " + puertoKafka);
        //Primer intento de conexión con el monitor, en el hilo del planificador
        scheduler.execute(this::saludMonitor);
        //Consumidor de kafka para esperar solicitudes de servicio de la central
    }
    /**
     * Alterna el estado de avería de la WS. Si estaba en avería, pasa a disponible; si estaba disponible, pasa a avería.
     */
    public void alternarAveria(){
        averiado = !averiado;
        if(averiado){
            log("Avería simulada activada. Se enviará KO al monitor");
        }
        else{
            log("Avería simulada resuelta. Se enviará OK al monitor");
        }
    }
    /**
     * UN intento de conectarse al monitor y responder a sus peticiones de salud.
     * Si la conexión falla o se pierde, programa el siguiente intento dentro de 3 s y termina:
     * sin bucle exterior, el método no vuelve a empezar por su cuenta (eso lo hace el planificador).
     */
    private void saludMonitor(){
        clienteMonitor = new SocketClient();
        try{
            clienteMonitor.connect(ipMonitor, puertoMonitor);
            log("Conectado al monitor en " + ipMonitor + ":" + puertoMonitor);
            //Bucle para responder a las peticiones de salud del monitor.
            //receiveMessage bloquea esperando un mensaje: es esperar un evento, no una pausa fija
            while(true){
                String mensaje = clienteMonitor.receiveMessage();
                if(mensaje==null){
                    throw new IOException("El monitor ha cerrado la conexión");
                }
                String respuesta;
                if(averiado){
                    respuesta = "KO";
                }
                else{
                    respuesta = "OK";
                }
                clienteMonitor.sendMessage(respuesta);
            }
        }catch(IOException e){
            log("Pérdida de conexión con el monitor: " + e.getMessage() + ". Reintentando en 3s...");
        }finally{
            if(clienteMonitor!=null){
                clienteMonitor.disconnect();
            }
            if(!scheduler.isShutdown()){
                scheduler.schedule(this::saludMonitor, 3, TimeUnit.SECONDS);
            }
        }
    }
    private void log(String mensaje){
        System.out.println("[" + LocalTime.now().withNano(0) + "] [ENGINE]" + mensaje);
    }
}
