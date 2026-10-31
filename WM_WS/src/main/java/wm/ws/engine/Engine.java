package wm.ws.engine;
import wm.common.interfaces.INetworkClient;
import wm.common.kafka.Topics;
import wm.common.network.SocketClient;

import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.internals.Topic;
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
    private volatile boolean regando = false;
    /**
     * Conexión actual con el monitor, ya autenticada. null mientras no estemos conectados.
     */
    private INetworkClient clienteMonitor;
    private final KafkaEngineManager kafkaManager;
    private String idWS = null;
    /**
     * Programador de tareas para reintentos de conexión al monitor
     */
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> tareaTelemetria;
    /**
     * Indica si la WS está conectada al monitor o no.
     * Se usa para que negarse a regar si no hay conexión con el monitor.
     */
    private volatile boolean monitorConectado = false;

    //Datos de telemetría de riego
    private double caudalLitrosMin = 10.0;
    private double volumenAcumuladoLitros = 0.0;
    private int tiempoRestanteSegundos = 0;
    private int duracionTotalSegundos = 0;

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
        this.kafkaManager = new KafkaEngineManager(puertoKafka);
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
            monitorConectado = true;
            log("Conectado al monitor en " + ipMonitor + ":" + puertoMonitor);
            String primerMensaje = clienteMonitor.receiveMessage();
            if(primerMensaje == null || !primerMensaje.startsWith("IDWS#")){
                throw new IOException("Respuesta inicial no válida del Monitor: " + primerMensaje);
            }
            this.idWS = primerMensaje.split("#")[1];
            log("Identificador asignado por el monitor: "+ idWS);
            iniciarEscuchaKafka();
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
            if(regando){
                monitorConectado = false;
                detenerRiego("PERDIDA_CONEXION_MONITOR");
            }
            if(clienteMonitor!=null){
                clienteMonitor.disconnect();
            }
            if(!scheduler.isShutdown()){
                scheduler.schedule(this::saludMonitor, 3, TimeUnit.SECONDS);
            }
        }
    }
    private void iniciarEscuchaKafka(){
        log("Iniciando consumidor de kafka para idWS: "+ idWS);
        kafkaManager.iniciarConsumidor(idWS, (clave, orden) -> {
            log("Orden recibida desde central: "+ orden);
        });
    }

    private void procesarOrden(String orden){
        String[] partes = orden.split("#");
        String accion = partes[0];
        switch(accion){
            case "INICIAR":
                int duracion;
                if(partes.length>1){
                    duracion=Integer.parseInt(partes[1]);
                }
                else{
                    duracion=60;
                }
                iniciarRiego(duracion);
                break;
            case "DETENER":
                detenerRiego("ORDEN_CENTRAL");
                break;
            default:
                log("Orden desconocida recibida por Kafka: " + orden);
        }
    }

    private synchronized void iniciarRiego(int duracionSegundos){
        if(!monitorConectado || averiado){
            log("Petición de riego RECHAZADA. Monitor conectado: " + monitorConectado + ", Averiado: " + averiado);
            kafkaManager.enviarMensaje(Topics.FIN_RIEGO, "RECHAZADO#ESTACION_NO_DISPONIBLE");
            return;
        }
        if(regando){
            log("La estación ya está regando actualmente");
            return;
        }
        regando = true;
        tiempoRestanteSegundos = duracionSegundos;
        volumenAcumuladoLitros = 0.0;
        log("Abirendo electroválvula. Riego iniciado por "+ duracionSegundos + " segundos");
        kafkaManager.enviarMensaje(Topics.INICIO_RIEGO, "OK#" +duracionSegundos);
        tareaTelemetria = scheduler.scheduleAtFixedRate(this::emitirTelemetria, 1, 1, TimeUnit.SECONDS);
    }

    private void emitirTelemetria(){
        if(!regando || averiado || !monitorConectado){
            detenerRiego(averiado ? "AVERIA_DETECTADA" : "MONITOR_DESCONECTADO");
            return;
        }
        volumenAcumuladoLitros = volumenAcumuladoLitros + (caudalLitrosMin/60.0);
        tiempoRestanteSegundos--;
        String telemetria = String.format("%.2f#%.2f#%d", caudalLitrosMin, volumenAcumuladoLitros, tiempoRestanteSegundos);
        kafkaManager.enviarMensaje(Topics.ESTADO_RIEGO_WS, telemetria);
        if(tiempoRestanteSegundos <=0){
            detenerRiego("TIEMPO_FINALIZADO");
        }
    }

    private synchronized void detenerRiego(String motivo){
        if(!regando){
            return;
        }
        regando = false;
        if(tareaTelemetria!=null){
            tareaTelemetria.cancel(false);
        }
        int duracionReal = duracionTotalSegundos - Math.max(0, tiempoRestanteSegundos);
        log("Cerrando electroválvula. Riego detenido. Motivo: "+ motivo + ". Volumen final: " + String.format("%s#%.2f#%d", motivo, volumenAcumuladoLitros, duracionReal));

    }

    private void log(String mensaje){
        System.out.println("[" + LocalTime.now().withNano(0) + "] [ENGINE]" + mensaje);
    }
}
