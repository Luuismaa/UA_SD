package wm.central;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import wm.common.interfaces.IKafkaAdmin;
import wm.common.kafka.KafkaAdmin;
import wm.common.kafka.Topics;
import wm.common.network.SocketServer;

/**
 * Lógica de la Central, separada de cualquier interfaz: BD, registro de estaciones, Kafka y
 * servidor de sockets. Funciona igual con ventana (en un PC) que sin pantalla (en Railway/Docker),
 * y la usan tanto la ventana Swing como el panel web.
 */
public class CentralCore {

    //Segundos entre intentos de conexión con Kafka (cada intento ya tarda hasta 15 s por el timeout del AdminClient)
    private static final int REINTENTO_KAFKA_S = 5;
    //Cada cuánto se reenvía el estado de todas las estaciones a los FO (protocolo: cada 5 s)
    private static final int ESTADO_GLOBAL_S = 5;

    private final int puertoEscucha;
    private final String broker;
    private final String rutaDB;
    //Puerto del panel web (http://IP:puertoWeb)
    private final int puertoWeb;

    private final BaseDatos bd;
    private final RegistroEstaciones registro;
    //null hasta que Kafka está disponible (la Central atiende sockets aunque Kafka no esté)
    private volatile KafkaCentralManager kafkaManager;

    //Planificador de tareas: ningún hilo se queda parado esperando. En vez de dormir esperando,
    //le pedimos que ejecute una tarea más tarde y el hilo actual queda libre
    private final ScheduledExecutorService planificador = Executors.newScheduledThreadPool(2);

    //Nº de intento de conexión con Kafka (solo para mostrarlo en el log)
    private int intentosKafka = 0;

    public CentralCore(int puertoEscucha, String broker, String rutaDB, int puertoWeb) {
        this.puertoEscucha = puertoEscucha;
        this.broker = broker;
        this.rutaDB = rutaDB;
        this.puertoWeb = puertoWeb;
        this.bd = new BaseDatos(rutaDB);
        this.registro = new RegistroEstaciones(bd);
    }

    /**
     * Carga las estaciones de la BD y arranca Kafka (en segundo plano, con reintentos) y el
     * servidor de sockets (en su propio hilo). Vuelve enseguida.
     * @throws SQLException si no se puede abrir la BD (sin ella la Central no sabe qué WS existen)
     */
    public void arrancar() throws SQLException {
        bd.inicializar();
        for (BaseDatos.DatosEstacion datos : bd.cargarEstaciones()) {
            registro.registrar(datos.id(), datos.ubicacion(), datos.fueraServicio());
        }
        for (Estacion est : registro.getEstaciones()) {
            log("Estación registrada: " + est);
        }
        planificador.execute(this::inicializarKafka);
        //Hilo propio: el bucle de accept no termina nunca y no debe ocupar un hilo del planificador
        new Thread(this::iniciarSockets, "servidor-sockets").start();
        //Panel web: si falla (p. ej. puerto ocupado) la Central sigue funcionando sin él
        try {
            new PanelWeb(this, puertoWeb).arrancar();
        } catch (IOException e) {
            System.err.println("[PANEL WEB] No se ha podido arrancar en el puerto " + puertoWeb + ": " + e.getMessage());
        }
    }

    /**
     * Un intento de conectar con Kafka y crear los topics. Si falla, no espera: programa el
     * siguiente intento en el planificador y termina.
     */
    private void inicializarKafka() {
        intentosKafka++;
        try {
            IKafkaAdmin kafkaAdmin = new KafkaAdmin();
            kafkaAdmin.crearTopicsSiNoExisten(broker, Topics.TODOS);
            KafkaCentralManager k = new KafkaCentralManager(broker, registro, bd);
            registro.setKafkaManager(k);
            k.iniciarEscucha();
            kafkaManager = k;
            log("[KAFKA] Conectado con Kafka en " + broker);

            //Estado de todas las estaciones a los FO, cada 5 s (además de en cada cambio)
            planificador.scheduleAtFixedRate(() -> {
                for (Estacion est : registro.getEstaciones()) {
                    k.publicarEstado(est);
                }
            }, 0, ESTADO_GLOBAL_S, TimeUnit.SECONDS);
        } catch (Exception e) {
            log("[KAFKA] No se ha podido conectar con Kafka en " + broker + " (intento " + intentosKafka + "). Reintentando en " + REINTENTO_KAFKA_S + " s...");
            planificador.schedule(this::inicializarKafka, REINTENTO_KAFKA_S, TimeUnit.SECONDS);
        }
    }

    private void iniciarSockets() {
        SocketServer servidor = new SocketServer(new ManejadorMonitor(registro));
        try {
            servidor.startServer(puertoEscucha);
        } catch (IOException e) {
            System.err.println("[CENTRAL] No se puede escuchar en el puerto " + puertoEscucha + ": " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Riego ordenado desde la Central (punto 11): SOLICITUD#CENTRAL#idWS#duracion.
     * Pasa por la misma validación que la petición de un operario.
     * @return false si Kafka todavía no está disponible
     */
    public boolean solicitarRiego(String idWS, int duracionS) {
        KafkaCentralManager k = kafkaManager;
        if (k == null) {
            return false;
        }
        log("Riego solicitado desde la Central en " + idWS + " (" + duracionS + " s)");
        k.publicarEvento(Topics.SOLICITAR_RIEGO, idWS, "SOLICITUD#CENTRAL#" + idWS + "#" + duracionS);
        return true;
    }

    public void bloquear(String idWS) {
        log("Bloqueo de " + idWS + " ordenado desde la Central");
        registro.bloquear(idWS);
    }

    public void activar(String idWS) {
        log("Activación de " + idWS + " ordenada desde la Central");
        registro.activar(idWS);
    }

    public RegistroEstaciones getRegistro() {
        return registro;
    }

    public boolean isKafkaConectado() {
        return kafkaManager != null;
    }

    public String getBroker() {
        return broker;
    }

    public int getPuertoEscucha() {
        return puertoEscucha;
    }

    public String getRutaDB() {
        return rutaDB;
    }

    private void log(String texto) {
        System.out.println("[" + LocalTime.now().withNano(0) + "] [CENTRAL] " + texto);
    }
}
