package wm.central;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import wm.common.kafka.Topics;

/**
 * Parte Kafka de la Central: el flujo de riego completo.
 *
 * Lee:     solicitarRiego, inicioRiego, estadoRiegoWS, finRiego
 * Publica: ordenesRiego, riegoPasosAutorizacion, estadoRiegoFO, resumenRiego, fugaRiego, estadoGlobalWS
 *
 * Flujo (protocolo, flujo 1):
 *   SOLICITUD -> PASO RECIBIDA, VALIDANDO -> (validar FO y WS) -> DENEGADA, o bien
 *   INICIAR a la WS + PASO ESPERANDO_WS -> INICIO de la WS -> REGANDO + PASO AUTORIZADA
 *   -> TELEMETRIA cada segundo (se reenvía al FO) -> FIN -> RESUMEN al FO + riego guardado en BD.
 *
 * Todos los mensajes se procesan en UN único hilo (el del consumidor), así que dos peticiones
 * para la misma WS nunca se validan a la vez.
 */
public class KafkaCentralManager {
    //Si la WS no confirma con INICIO en este tiempo, el riego se deniega
    private static final int TIMEOUT_INICIO_S = 5;
    //Una SOLICITUD (o un INICIO) más antiguo que esto se publicó con la Central caída
    private static final long ANTIGUEDAD_MAX_SOLICITUD_MS = 10_000;
    //Tras resolverse una fuga con un riego sin cerrar, tiempo que se espera el FIN del Engine
    private static final int MARGEN_FIN_TRAS_FUGA_S = 5;

    private final String broker;
    private final RegistroEstaciones registro;
    private final BaseDatos bd;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    //Riegos en marcha o pendientes de confirmar: idWS -> solicitante (FO-xx, MANUAL o CENTRAL)
    private final Map<String, String> regandoOperarios = new ConcurrentHashMap<>();
    //WS a las que se ha enviado INICIAR y aún no han respondido con INICIO
    private final Set<String> pendientesInicio = ConcurrentHashMap.newKeySet();
    //Riego en curso de cada WS: idWS -> id del riego en la BD
    private final Map<String, Long> riegosBD = new ConcurrentHashMap<>();
    //Hora de inicio de cada riego (para calcular la duración si la Central lo cierra ella misma)
    private final Map<String, LocalDateTime> iniciosRiego = new ConcurrentHashMap<>();
    //Riegos cuyo operario ya ha recibido el aviso de fuga (para no avisarle dos veces)
    private final Set<String> fugasAvisadas = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public KafkaCentralManager(String broker, RegistroEstaciones registro, BaseDatos bd){
        this.broker=broker;
        this.registro=registro;
        this.bd = bd;
        initProducer();
    }

    private void initProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Si Kafka cae, no bloquear los hilos de la Central más de 3 s en un send()
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        this.producer = new KafkaProducer<>(props);
    }

    public void publicarEvento(String topic, String key, String valor){
        producer.send(new ProducerRecord<>(topic, key, valor), (meta, error) -> {
            if (error != null) {
                log("No se ha podido publicar en " + topic + ": " + error.getMessage());
            }
        });
    }

    /**
     * ESTADO_WS#idWS#ubicacion#estado en estadoGlobalWS (en cada cambio y cada 5 s).
     */
    public void publicarEstado(Estacion est){
        publicarEvento(Topics.ESTADO_GLOBAL_WS, est.getId(),
            "ESTADO_WS#" + est.getId() + "#" + est.getUbicacion() + "#" + est.getEstado().name());
    }

    public void iniciarEscucha(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        //group.id fijo (protocolo): si la Central se reinicia, sigue donde lo dejó y procesa los
        //FIN que llegaron mientras estaba caída (las SOLICITUD viejas se rechazan por antigüedad)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //Si la Central se cae de golpe, Kafka tarda session.timeout.ms en dar por muerto al consumidor
        //anterior, y mientras tanto la Central reiniciada no recibe nada. Por defecto son 45 s;
        //6 s es el mínimo que admite el broker
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2000);

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(
            Topics.SOLICITAR_RIEGO,
            Topics.INICIO_RIEGO,
            Topics.ESTADO_RIEGO_WS,
            Topics.FIN_RIEGO
        ));
        Thread hilo = new Thread(() -> {
            try{
                while(running){
                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(300));
                    for(ConsumerRecord<String,String> record: records){
                        //Un mensaje mal formado se descarta y se registra: nunca tumba el consumidor
                        try {
                            procesarMensaje(record);
                        } catch (RuntimeException e) {
                            log("Mensaje descartado en " + record.topic() + " ('" + record.value() + "'): " + e);
                        }
                    }
                }
            }catch(Exception e){
                if(running){
                    log("Error en el consumidor: " + e.getMessage());
                }
            }finally{
                consumer.close();
            }
        }, "central-kafka-listener");
        hilo.start();
    }

    private void procesarMensaje(ConsumerRecord<String,String> record){
        String mensaje = record.value();
        switch(record.topic()){
            case Topics.SOLICITAR_RIEGO -> procesarSolicitudRiego(mensaje, record.timestamp());
            case Topics.INICIO_RIEGO -> procesarInicioRiego(mensaje, record.timestamp());
            case Topics.ESTADO_RIEGO_WS -> procesarTelemetriaWS(mensaje);
            case Topics.FIN_RIEGO -> procesarFinRiego(mensaje);
        }
    }

    /**
     * SOLICITUD#solicitante#idWS#duracion  (solicitante = FO-xx, MANUAL o CENTRAL)
     */
    private void procesarSolicitudRiego(String mensaje, long timestamp){
        String[] partes = mensaje.split("#", -1);
        if(partes.length != 4 || !partes[0].equals("SOLICITUD")){
            log("SOLICITUD con formato incorrecto, descartada: '" + mensaje + "'");
            return;
        }
        String solicitante = partes[1];
        String idWS = partes[2];
        log("Solicitud de riego de " + solicitante + " para " + idWS + " (" + partes[3] + " s)");
        paso(solicitante, idWS, "RECIBIDA", "");

        //Publicada mientras la Central estaba caída: no se admiten peticiones nuevas (guía)
        if(System.currentTimeMillis() - timestamp > ANTIGUEDAD_MAX_SOLICITUD_MS){
            paso(solicitante, idWS, "DENEGADA", "WM_Central no estaba disponible");
            return;
        }

        paso(solicitante, idWS, "VALIDANDO", "");
        int duracion;
        try {
            duracion = Integer.parseInt(partes[3]);
        } catch (NumberFormatException e) {
            duracion = -1;
        }
        String motivo = duracion <= 0 ? "Duración no válida" : validar(solicitante, idWS);
        if(motivo != null){
            paso(solicitante, idWS, "DENEGADA", motivo);
            return;
        }

        //Todo correcto: reservar la WS, enviar la orden y esperar su INICIO
        regandoOperarios.put(idWS, solicitante);
        pendientesInicio.add(idWS);
        publicarEvento(Topics.ORDENES_RIEGO, idWS, "INICIAR#" + idWS + "#" + solicitante + "#" + duracion);
        paso(solicitante, idWS, "ESPERANDO_WS", "");

        scheduler.schedule(()->{
            //remove devuelve true solo si seguía pendiente (no ha llegado INICIO)
            if(pendientesInicio.remove(idWS)){
                regandoOperarios.remove(idWS);
                paso(solicitante, idWS, "DENEGADA", "La estación no ha respondido");
            }
        }, TIMEOUT_INICIO_S, TimeUnit.SECONDS);
    }

    /**
     * Comprueba si se puede regar. Devuelve null si todo está bien, o el motivo de la denegación.
     */
    private String validar(String solicitante, String idWS){
        if(esOperario(solicitante) && !bd.existeOperario(solicitante)){
            return "Operario no registrado";
        }
        Estacion est = registro.getEstacion(idWS);
        if(est == null){
            return "Estación no registrada";
        }
        if(regandoOperarios.containsKey(idWS)){
            return "La estación ya está ocupada";
        }
        return switch(est.getEstado()){
            case DISPONIBLE -> null;
            case REGANDO -> "La estación ya está regando";
            case FUGA -> "La estación tiene una fuga";
            case FUERA_SERVICIO -> "La estación está fuera de servicio";
            case DESCONECTADA -> "La estación está desconectada";
        };
    }

    /**
     * INICIO#idWS#solicitante#fechaHora: la WS ha abierto la electroválvula.
     */
    private void procesarInicioRiego(String mensaje, long timestamp){
        String[] partes = mensaje.split("#", -1);
        if(partes.length != 4 || !partes[0].equals("INICIO")){
            log("INICIO con formato incorrecto, descartado: '" + mensaje + "'");
            return;
        }
        String idWS = partes[1];
        //INICIO antiguo que se lee al reiniciar la Central: ese riego ya está guardado en la BD (o
        //acabó). No se ordena PARAR: la guía dice que las WS siguen regando aunque caiga la Central
        if(System.currentTimeMillis() - timestamp > ANTIGUEDAD_MAX_SOLICITUD_MS){
            log("INICIO antiguo de " + idWS + " (de antes de arrancar la Central): se ignora");
            return;
        }
        String solicitante = regandoOperarios.get(idWS);
        //Llega tarde (ya se denegó por timeout) o nadie lo pidió: la Central manda parar
        if(!pendientesInicio.remove(idWS) || solicitante == null){
            log("INICIO inesperado de " + idWS + ": se ordena PARAR");
            publicarEvento(Topics.ORDENES_RIEGO, idWS, "PARAR#" + idWS + "#CENTRAL");
            return;
        }
        //Entre la orden y el INICIO le puede haber entrado una fuga o un bloqueo
        if(!registro.iniciarRiego(idWS, solicitante)){
            log(idWS + " ya no está disponible al confirmar el inicio: se ordena PARAR");
            regandoOperarios.remove(idWS);
            publicarEvento(Topics.ORDENES_RIEGO, idWS, "PARAR#" + idWS + "#CENTRAL");
            paso(solicitante, idWS, "DENEGADA", "La estación ha dejado de estar disponible");
            return;
        }
        iniciosRiego.put(idWS, LocalDateTime.now());
        long idRiego = bd.iniciarRiego(idWS, solicitante, partes[3]);
        if(idRiego > 0){
            riegosBD.put(idWS, idRiego);
        }
        paso(solicitante, idWS, "AUTORIZADA", "");
    }

    /**
     * TELEMETRIA#idWS#caudal#volumen: se muestra en el panel y se reenvía al FO como
     * TELEMETRIA#idFO#idWS#caudal#volumen.
     */
    private void procesarTelemetriaWS(String mensaje){
        String[] partes = mensaje.split("#", -1);
        if(partes.length != 4 || !partes[0].equals("TELEMETRIA")){
            log("TELEMETRIA con formato incorrecto, descartada: '" + mensaje + "'");
            return;
        }
        String idWS = partes[1];
        String solicitante = solicitanteDe(idWS);
        //Tras un reinicio de la Central, la WS se autentica como DISPONIBLE aunque siga regando:
        //si la BD dice que tiene un riego abierto, se vuelve a mostrar como REGANDO
        Estacion est = registro.getEstacion(idWS);
        if(solicitante != null && est != null && est.getEstado() == wm.common.protocol.EstadoWS.DISPONIBLE){
            registro.iniciarRiego(idWS, solicitante);
        }
        registro.actualizarTelemetria(idWS, Double.parseDouble(partes[2]), Double.parseDouble(partes[3]));
        if(esOperario(solicitante)){
            publicarEvento(Topics.ESTADO_RIEGO_FO, solicitante,
                "TELEMETRIA#" + solicitante + "#" + idWS + "#" + partes[2] + "#" + partes[3]);
        }
    }

    /**
     * FIN#idWS#motivo#volumenTotal#duracion
     */
    private void procesarFinRiego(String mensaje){
        String[] partes = mensaje.split("#", -1);
        if(partes.length != 5 || !partes[0].equals("FIN")){
            log("FIN con formato incorrecto, descartado: '" + mensaje + "'");
            return;
        }
        cerrarRiego(partes[1], partes[2], Double.parseDouble(partes[3]), Integer.parseInt(partes[4]));
    }

    /**
     * Cierra un riego: lo guarda en la BD, envía el RESUMEN al operario y devuelve la WS a su estado.
     * RESUMEN#idFO#idWS#volumenTotal#duracion#motivo
     */
    private void cerrarRiego(String idWS, String motivo, double volumen, int duracion){
        String solicitante = solicitanteDe(idWS);
        regandoOperarios.remove(idWS);
        pendientesInicio.remove(idWS);
        iniciosRiego.remove(idWS);
        boolean fugaYaAvisada = fugasAvisadas.remove(idWS);

        Long idRiego = riegosBD.remove(idWS);
        if(solicitante == null && idRiego == null){
            //Ese riego ya estaba cerrado (p. ej. la Central lo cerró al perder el Monitor y después
            //llega el FIN del Engine): no hay nada más que hacer
            log("FIN de " + idWS + " (motivo " + motivo + ") de un riego ya cerrado: se ignora");
            return;
        }
        if(idRiego != null){
            bd.finalizarRiego(idRiego, LocalDateTime.now().withNano(0).toString(), duracion, volumen, motivo);
        }

        String volumenTxt = String.format(Locale.ROOT, "%.1f", volumen);
        log("Fin de riego en " + idWS + " (" + solicitante + "): " + volumenTxt + " L en " + duracion + " s, motivo " + motivo);
        boolean porFuga = motivo.equals("FUGA");
        if(esOperario(solicitante)){
            //El Engine puede cortar el riego por la avería antes de que llegue el aviso del Monitor:
            //el operario recibe igualmente el aviso de fuga antes de su resumen
            if(porFuga && !fugaYaAvisada){
                publicarEvento(Topics.FUGA_RIEGO, solicitante, "FUGA#" + solicitante + "#" + idWS + "#" + volumenTxt);
            }
            publicarEvento(Topics.RESUMEN_RIEGO, solicitante,
                "RESUMEN#" + solicitante + "#" + idWS + "#" + volumenTxt + "#" + duracion + "#" + motivo);
        }
        registro.finalizarRiego(idWS, porFuga);
    }

    /**
     * Llamado por el registro cuando el Monitor avisa de una fuga.
     * Riego en curso: PARAR#idWS#FUGA y FUGA#idFO#idWS#volumen (el Engine responderá con FIN -> RESUMEN).
     * Riego pendiente de INICIO: se deniega.
     */
    public void alDetectarFuga(String idWS){
        String solicitante = regandoOperarios.get(idWS);
        if(solicitante == null){
            return;
        }
        publicarEvento(Topics.ORDENES_RIEGO, idWS, "PARAR#" + idWS + "#FUGA");
        if(pendientesInicio.remove(idWS)){
            regandoOperarios.remove(idWS);
            paso(solicitante, idWS, "DENEGADA", "La estación tiene una fuga");
            return;
        }
        if(esOperario(solicitante)){
            Estacion est = registro.getEstacion(idWS);
            double volumen = est != null ? est.getVolumen() : 0.0;
            publicarEvento(Topics.FUGA_RIEGO, solicitante,
                "FUGA#" + solicitante + "#" + idWS + "#" + String.format(Locale.ROOT, "%.1f", volumen));
            fugasAvisadas.add(idWS);
        }
    }

    /**
     * Llamado por el registro cuando se pierde el Monitor de una WS. Si estaba regando, la
     * Central cierra el riego con los últimos datos recibidos y motivo CENTRAL (flujo 5).
     */
    public void alDesconectarEstacion(String idWS){
        if(!tieneRiegoEnCurso(idWS)){
            //Sin riego, o aún no había empezado (el timeout de 5 s lo denegará)
            return;
        }
        cerrarConUltimosDatos(idWS, "CENTRAL");
    }

    /**
     * Llamado por el registro cuando el Monitor avisa de que la fuga se ha resuelto.
     * Si el riego sigue abierto es porque el Engine se cayó a mitad y nunca envió su FIN.
     * Se le da un margen para que, al recuperarse, envíe el estado final del riego (guía);
     * si no lo hace, la Central lo cierra con los últimos datos recibidos, avisa al FO con el
     * RESUMEN y la estación queda libre para nuevos riegos.
     */
    public void alResolverFuga(String idWS){
        if(!tieneRiegoEnCurso(idWS)){
            return;
        }
        log(idWS + ": fuga resuelta con un riego sin cerrar (el Engine pudo caerse). Esperando su FIN " + MARGEN_FIN_TRAS_FUGA_S + " s...");
        scheduler.schedule(() -> {
            if(tieneRiegoEnCurso(idWS)){
                log(idWS + ": el Engine no ha enviado el FIN, la Central cierra el riego con los últimos datos");
                cerrarConUltimosDatos(idWS, "FUGA");
            }
        }, MARGEN_FIN_TRAS_FUGA_S, TimeUnit.SECONDS);
    }

    /**
     * ¿Hay un riego ya empezado (con INICIO) en esta estación?
     */
    private boolean tieneRiegoEnCurso(String idWS){
        return regandoOperarios.containsKey(idWS) && !pendientesInicio.contains(idWS);
    }

    /**
     * Cierra el riego con lo último que se sabe: el volumen de la última telemetría y el tiempo
     * transcurrido desde el INICIO. Se usa cuando la WS ya no puede enviar su FIN.
     */
    private void cerrarConUltimosDatos(String idWS, String motivo){
        Estacion est = registro.getEstacion(idWS);
        double volumen = est != null ? est.getVolumen() : 0.0;
        LocalDateTime inicio = iniciosRiego.get(idWS);
        int duracion = inicio != null ? (int) Duration.between(inicio, LocalDateTime.now()).getSeconds() : 0;
        cerrarRiego(idWS, motivo, volumen, duracion);
    }

    /**
     * Quién pidió el riego en curso de una WS. Si la Central se ha reiniciado y no lo tiene en
     * memoria, lo busca en la BD (riego sin cerrar) para poder seguir avisando a su operario.
     */
    private String solicitanteDe(String idWS){
        String solicitante = regandoOperarios.get(idWS);
        if(solicitante == null){
            BaseDatos.RiegoAbierto abierto = bd.buscarRiegoAbierto(idWS);
            if(abierto != null){
                solicitante = abierto.solicitante();
                regandoOperarios.put(idWS, solicitante);
                riegosBD.put(idWS, abierto.id());
                //Hora de inicio guardada en la BD, para calcular la duración si hay que cerrarlo
                try {
                    iniciosRiego.putIfAbsent(idWS, LocalDateTime.parse(abierto.inicio()));
                } catch (RuntimeException e) {
                    //Fecha con otro formato: la duración se calculará como 0
                }
            }
        }
        return solicitante;
    }

    /**
     * PASO#idFO#idWS#paso#detalle al operario (solo si lo pidió un FO) y al log de la Central.
     */
    private void paso(String solicitante, String idWS, String paso, String detalle){
        log(idWS + " (" + solicitante + "): " + paso + (detalle.isEmpty() ? "" : " - " + detalle));
        if(esOperario(solicitante)){
            publicarEvento(Topics.RIEGO_PASOS_AUTORIZACION, solicitante,
                "PASO#" + solicitante + "#" + idWS + "#" + paso + "#" + detalle);
        }
    }

    /**
     * MANUAL (menú de la estación) y CENTRAL no son operarios: no se les envían mensajes.
     */
    private static boolean esOperario(String solicitante){
        return solicitante != null && !solicitante.equals("MANUAL") && !solicitante.equals("CENTRAL");
    }

    public String getOperarioRegando(String idWS){
        return regandoOperarios.get(idWS);
    }

    public void detener(){
        running = false;
        scheduler.shutdown();
        if(producer != null){
            producer.close();
        }
    }

    private void log(String texto){
        System.out.println("[" + java.time.LocalTime.now().withNano(0) + "] [RIEGO] " + texto);
    }
}
