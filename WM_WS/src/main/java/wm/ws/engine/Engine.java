package wm.ws.engine;
import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import wm.common.interfaces.INetworkClient;
import wm.common.kafka.Topics;
import wm.common.network.SocketClient;
/**
 * Clase principal del Engine de la WS (WM_WS_E).
 * Se encarga de iniciar el monitor y de conectarse a Kafka.
 * Engine
 */
public class Engine {
    private boolean kafkaIniciado = false;
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
    private volatile boolean bloqueada = false;
    private String operarioActual = "";
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
            if(!kafkaIniciado){
                iniciarEscuchaKafka();
                kafkaIniciado = true;
            }
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
                detenerRiego("MONITOR_DESCONECTADO");
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
            procesarOrden(orden);
        });
    }

    private void procesarOrden(String orden){
        String[] partes = orden.split("#");
        String accion = partes[0];
        //Verificamos que el mensaje para la estación, comprobando el formato
        if(partes.length> 1 && !partes[1].equals(this.idWS)){
            return;
        }
        switch(accion){
            case "INICIAR":
                //Formato: INICIAR#WS-03#FO-12#60
                if(!bloqueada){
                    String idFO;
                    if(partes.length > 2){
                        idFO=partes[2];
                    }
                    else{
                        idFO = "DESCONOCIDO";
                    }
                    int duracion = 60;
                    if(partes.length > 3){
                        try {
                            duracion = Integer.parseInt(partes[3]);
                        } catch (NumberFormatException e) {
                            log("Error al parsear duración, se usará 60s por defecto.");
                        }
                    }
                    iniciarRiego(duracion, idFO);
                }
                break;
            case "PARAR":
                // Formato: PARAR#WS-03#FUGA
                String motivo;
                if(partes.length>2){
                    motivo = partes[2];
                }
                else{
                    motivo="CENTRAL";
                }
                detenerRiego(motivo);
                break;
            case "BLOQUEAR":
                //Formato: BLOQUEAR#WS-03
                bloqueada = true;
                detenerRiego("BLOQUEO");
                break;
            case "ACTIVAR":
                //Formato: ACTIVAR#WS-03
                bloqueada = false;
                log("Estación desbloqueada y lista");
                break;
            default:
                log("Orden desconocida recibida por Kafka: " + orden);
        }
    }

    private synchronized void iniciarRiego(int duracionSegundos, String idFO){
        if(!monitorConectado || averiado || regando || bloqueada){
            log("Petición de riego ignorada. Monitor conectado: " + monitorConectado + ", Averiado: " + averiado + ", BLoqueada: " + bloqueada);
            return;
        }
        regando = true;
        tiempoRestanteSegundos = duracionSegundos;
        duracionTotalSegundos = duracionSegundos;
        volumenAcumuladoLitros = 0.0;
        operarioActual = idFO;
        String timestamp = java.time.LocalDateTime.now().withNano(0).toString();
        //Formato: INICIO#WS-03#FO-12#2026-10-31T12:04:32
        String mensajeInicio = String.format("INICIO#%s#%s#%s", idWS, idFO, timestamp);

        log("Abirendo electroválvula. Riego iniciado por "+ duracionSegundos + " segs (Operario: " + idFO + ")");
        kafkaManager.enviarMensaje(Topics.INICIO_RIEGO, mensajeInicio);
        tareaTelemetria = scheduler.scheduleAtFixedRate(this::emitirTelemetria, 1, 1, TimeUnit.SECONDS);
    }

    private void emitirTelemetria(){
        if(!regando || averiado || !monitorConectado){
            detenerRiego(averiado ? "FUGA" : "MONITOR_DESCONECTADO");
            return;
        }
        volumenAcumuladoLitros = volumenAcumuladoLitros + (caudalLitrosMin/60.0);
        tiempoRestanteSegundos--;
        // Formato: TELEMETRIA#WS-03#12.4#186.0 (Locale.US para el punto decimal)
        String telemetria = String.format(java.util.Locale.US, "TELEMETRIA#%s#%.1f#%.1f",
            idWS, caudalLitrosMin, volumenAcumuladoLitros);
        
        kafkaManager.enviarMensaje(Topics.ESTADO_RIEGO_WS, telemetria);
        if(tiempoRestanteSegundos <=0){
            detenerRiego("TIEMPO");
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
        
        //Formato: FIN#WS-03#TIEMPO#620.0#60
        String mensajeFin = String.format(java.util.Locale.US, "FIN#%s#%s#%.1f#%d", 
            idWS, motivo, volumenAcumuladoLitros, duracionReal);
        
        log("Cerrando electroválvula. Riego detenido. Motivo: "+ motivo + "-> Mensaje enviado: " + mensajeFin);
        kafkaManager.enviarMensaje(Topics.FIN_RIEGO, mensajeFin);
    }

    private void log(String mensaje){
        System.out.println("[" + LocalTime.now().withNano(0) + "] [ENGINE]" + mensaje);
    }
    public void detener(){
        log("Apagando Engine...");
        if(scheduler != null && !scheduler.isShutdown()){
            scheduler.shutdown();
        }
        if(kafkaManager != null){
            kafkaManager.detener();
        }
    }

    public void iniciarRiegoManual(int duracionSegundos){
        log("Procesando SOLICITUD#MANUAL localmente...");
        iniciarRiego(duracionSegundos, "MANUAL");
    }

    public void detenerRiegoManual(){
        log("Procesando parada manual...");
        detenerRiego("MANUAL");
    }
}
